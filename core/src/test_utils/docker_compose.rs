/*
 * Copyright 2025 BergLoom
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

use core::net::{IpAddr, SocketAddr};
use std::{collections::HashMap, process::Command, sync::RwLock};

use ctor::{ctor, dtor};
use iceberg_catalog_rest::{RestCatalog, RestCatalogConfig};
use port_scanner::scan_port_addr;

// Copyright https://github.com/apache/iceberg-rust/crates/test_util. Licensed under Apache-2.0.

const REST_CATALOG_PORT: u16 = 8181;
const REST_SERVICE: &str = "rest";
const MINIO_SERVICE: &str = "minio";

const AWS_ACCESS_KEY_ID: &str = "AWS_ACCESS_KEY_ID";
const AWS_SECRET_ACCESS_KEY: &str = "AWS_SECRET_ACCESS_KEY";
const AWS_REGION: &str = "AWS_REGION";
const MINIO_API_PORT: &str = "MINIO_API_PORT";

const S3_ACCESS_KEY_ID: &str = "s3.access-key-id";
const S3_SECRET_ACCESS_KEY: &str = "s3.secret-access-key";
const S3_REGION: &str = "s3.region";
const S3_ENDPOINT: &str = "s3.endpoint";

const DEFAULT_ADMIN: &str = "admin";
const DEFAULT_PASSWORD: &str = "password";
const DEFAULT_REGION: &str = "us-east-1";
const DEFAULT_MINIO_PORT: &str = "9000";
const DEFAULT_PLATFORM_ENV: &str = "DOCKER_DEFAULT_PLATFORM";

static DOCKER_COMPOSE_ENV: RwLock<Option<DockerCompose>> = RwLock::new(None);

#[ctor]
fn before_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    let docker_compose = DockerCompose::new(
        module_path!().replace("::", "__").replace('.', "_"),
        format!("{}/testdata", env!("CARGO_MANIFEST_DIR")),
    );
    docker_compose.up();
    guard.replace(docker_compose);
}

#[dtor]
fn after_all() {
    let mut guard = DOCKER_COMPOSE_ENV.write().unwrap();
    if let Some(d) = guard.take() { d.down() }
}

pub async fn get_rest_catalog() -> RestCatalog {
    let (rest_catalog_ip, props) = {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        let aws_access_key_id = docker_compose.get_container_env_value(REST_SERVICE, AWS_ACCESS_KEY_ID);
        let aws_secret_access_key =
            docker_compose.get_container_env_value(REST_SERVICE, AWS_SECRET_ACCESS_KEY);
        let aws_region = docker_compose.get_container_env_value(REST_SERVICE, AWS_REGION);
        let minio_ip = docker_compose.get_container_ip(MINIO_SERVICE);
        let minio_port = docker_compose
            .get_container_env_value(MINIO_SERVICE, MINIO_API_PORT)
            .unwrap_or(DEFAULT_MINIO_PORT.to_string());
        let aws_endpoint = format!("http://{}:{}", minio_ip, minio_port);
        let props = HashMap::from([
            (
                S3_ACCESS_KEY_ID.to_string(),
                aws_access_key_id.unwrap_or(DEFAULT_ADMIN.to_string()),
            ),
            (
                S3_SECRET_ACCESS_KEY.to_string(),
                aws_secret_access_key.unwrap_or(DEFAULT_PASSWORD.to_string()),
            ),
            (
                S3_REGION.to_string(),
                aws_region.unwrap_or(DEFAULT_REGION.to_string()),
            ),
            (S3_ENDPOINT.to_string(), aws_endpoint),
        ]);
        let rest_catalog_ip = docker_compose.get_container_ip(REST_SERVICE);
        (rest_catalog_ip, props)
    };

    let rest_socket_addr = SocketAddr::new(rest_catalog_ip, REST_CATALOG_PORT);
    while !scan_port_addr(rest_socket_addr) {
        tracing::info!("Waiting for 1s rest catalog to ready...");
        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }

    let config = RestCatalogConfig::builder()
        .uri(format!("http://{}", rest_socket_addr))
        .props(props)
        .build();
    RestCatalog::new(config)
}

struct DockerCompose {
    project_name: String,
    docker_compose_dir: String,
}

impl DockerCompose {
    fn new(project_name: impl ToString, docker_compose_dir: impl ToString) -> Self {
        Self {
            project_name: project_name.to_string(),
            docker_compose_dir: docker_compose_dir.to_string(),
        }
    }

    fn get_os_arch() -> String {
        let mut cmd = Command::new("docker");
        cmd.arg("info")
            .arg("--format")
            .arg("{{.OSType}}/{{.Architecture}}");

        let result = get_cmd_output_result(cmd, "Get os arch".to_string());
        match result {
            Ok(value) => value.trim().to_string(),
            Err(_err) => {
                // docker/podman do not consistently place OSArch info in the same json path across OS and versions
                // Below tries an alternative path if the above path fails
                let mut alt_cmd = Command::new("docker");
                alt_cmd
                    .arg("info")
                    .arg("--format")
                    .arg("{{.Version.OsArch}}");
                get_cmd_output(alt_cmd, "Get os arch".to_string())
                    .trim()
                    .to_string()
            }
        }
    }

    pub fn up(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.env(DEFAULT_PLATFORM_ENV, Self::get_os_arch());

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "up",
            "-d",
            "--wait",
            "--timeout",
            "1200000",
        ]);
        println!("cmd: {:?}", cmd);

        run_command(
            cmd,
            format!(
                "Starting docker compose in {}, project name: {}",
                self.docker_compose_dir, self.project_name
            ),
        )
    }

    pub fn down(&self) {
        let mut cmd = Command::new("docker");
        cmd.current_dir(&self.docker_compose_dir);

        cmd.args(vec![
            "compose",
            "-p",
            self.project_name.as_str(),
            "down",
            "-v",
            "--remove-orphans",
        ]);

        run_command(
            cmd,
            format!(
                "Stopping docker compose in {}, project name: {}",
                self.docker_compose_dir, self.project_name
            ),
        )
    }

    pub fn get_container_ip(&self, service_name: impl AsRef<str>) -> IpAddr {
        let container_name = format!("{}-{}-1", self.project_name, service_name.as_ref());
        let mut cmd = Command::new("docker");
        cmd.arg("inspect")
            .arg("--format")
            .arg("{{range.NetworkSettings.Networks}}{{.IPAddress}}{{end}}")
            .arg(&container_name);

        let ip_result = get_cmd_output(cmd, format!("Get container ip of {container_name}"))
            .trim()
            .parse::<IpAddr>();
        match ip_result {
            Ok(ip) => ip,
            Err(e) => {
                tracing::error!("Invalid IP, {e}");
                panic!("Failed to parse IP for {container_name}")
            }
        }
    }

    pub fn get_container_env(&self, service_name: impl AsRef<str>) -> Vec<(String, String)> {
        let container_name = format!("{}-{}-1", self.project_name, service_name.as_ref());
        let mut cmd = Command::new("docker");
        cmd.arg("inspect")
            .arg("--format")
            .arg("{{range .Config.Env}}{{.}}{{\"\\n\"}}{{end}}")
            .arg(&container_name);

        let env_output = get_cmd_output(cmd, format!("Get container env of {container_name}"));

        let env_vars: Vec<(String, String)> = env_output
            .trim()
            .lines()
            .filter(|line| !line.is_empty())
            .filter_map(|line| {
                let parts: Vec<&str> = line.splitn(2, '=').collect();
                if parts.len() == 2 {
                    Some((parts[0].to_string(), parts[1].to_string()))
                } else {
                    None
                }
            })
            .collect();

        env_vars
    }

    pub fn get_container_env_value(
        &self,
        service_name: impl AsRef<str>,
        env_key: impl AsRef<str>,
    ) -> Option<String> {
        let env_vars = self.get_container_env(service_name);
        env_vars
            .into_iter()
            .find(|(key, _)| key == env_key.as_ref())
            .map(|(_, value)| value)
    }
}

pub fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    tracing::info!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        tracing::info!("{} succeed!", desc)
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

pub fn get_cmd_output_result(mut cmd: Command, desc: impl ToString) -> Result<String, String> {
    let desc = desc.to_string();
    tracing::info!("Starting to {}, command: {:?}", &desc, cmd);
    let result = cmd.output();
    match result {
        Ok(output) => {
            if output.status.success() {
                tracing::info!("{} succeed!", desc);
                Ok(String::from_utf8(output.stdout).unwrap())
            } else {
                Err(format!("{} failed with rc: {:?}", desc, output.status))
            }
        }
        Err(err) => Err(format!("{} failed with error: {}", desc, { err })),
    }
}

pub fn get_cmd_output(cmd: Command, desc: impl ToString) -> String {
    let result = get_cmd_output_result(cmd, desc);
    match result {
        Ok(output_str) => output_str,
        Err(err) => panic!("{}", err),
    }
}
