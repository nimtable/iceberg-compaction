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

use std::{path::PathBuf, process::Command};

async fn get_rest_catalog() -> RestCatalog {
    set_up();

    let rest_catalog_ip = {
        let guard = DOCKER_COMPOSE_ENV.read().unwrap();
        let docker_compose = guard.as_ref().unwrap();
        docker_compose.get_container_ip("rest")
    };

    let rest_socket_addr = SocketAddr::new(rest_catalog_ip, REST_CATALOG_PORT);
    while !scan_port_addr(rest_socket_addr) {
        log::info!("Waiting for 1s rest catalog to ready...");
        sleep(std::time::Duration::from_millis(1000)).await;
    }

    let config = RestCatalogConfig::builder()
        .uri(format!("http://{}", rest_socket_addr))
        .build();
    RestCatalog::new(config)
}

struct DockerCompose {
    project_name: String,
    compose_file: PathBuf,
}

impl DockerCompose {
    fn new(project_name: &str, compose_file: PathBuf) -> Self {
        Self {
            project_name: project_name.to_string(),
            compose_file,
        }
    }

    fn up(&self) -> std::io::Result<()> {
        Command::new("docker-compose")
            .arg("-f")
            .arg(&self.compose_file)
            .arg("-p")
            .arg(&self.project_name)
            .arg("up")
            .arg("-d")
            .status()?;
        run_command(cmd, format!("Starting docker compose in {}, project name: {}", self.docker_compose_dir, self.project_name));
        Ok(())
    }

    fn down(&self) -> std::io::Result<()> {
        Command::new("docker-compose")
            .arg("-f")
            .arg(&self.compose_file)
            .arg("-p")
            .arg(&self.project_name)
            .arg("down")
            .status()?;
        run_command(cmd, format!("Stopping docker compose in {}, project name: {}", self.docker_compose_dir, self.project_name));
        Ok(())
    }
}

pub fn run_command(mut cmd: Command, desc: impl ToString) {
    let desc = desc.to_string();
    log::info!("Starting to {}, command: {:?}", &desc, cmd);
    let exit = cmd.status().unwrap();
    if exit.success() {
        log::info!("{} succeed!", desc)
    } else {
        panic!("{} failed: {:?}", desc, exit);
    }
}

// pub fn get_cmd_output_result(mut cmd: Command, desc: impl ToString) -> Result<String, String> {
//     let desc = desc.to_string();
//     log::info!("Starting to {}, command: {:?}", &desc, cmd);
//     let result = cmd.output();
//     match result {
//         Ok(output) => {
//             if output.status.success() {
//                 log::info!("{} succeed!", desc);
//                 Ok(String::from_utf8(output.stdout).unwrap())
//             } else {
//                 Err(format!("{} failed with rc: {:?}", desc, output.status))
//             }
//         }
//         Err(err) => Err(format!("{} failed with error: {}", desc, { err })),
//     }
// }

// pub fn get_cmd_output(cmd: Command, desc: impl ToString) -> String {
//     let result = get_cmd_output_result(cmd, desc);
//     match result {
//         Ok(output_str) => output_str,
//         Err(err) => panic!("{}", err),
//     }
// }