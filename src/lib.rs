/********************************************************************************
 * Copyright (c) 2023 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use std::sync::Arc;

use paho_mqtt::{self as mqtt, MQTT_VERSION_5};
use protobuf::Message;
use up_rust::uprotocol::{UCode, UMessage, UStatus, UUri, UUID};

pub mod transport;

pub type UListener = Box<dyn Fn(Result<UMessage, UStatus>) + Send + Sync + 'static>;

pub struct MqttConfig {
    pub mqtt_port: String,
    pub mqtt_namespace: String,
    pub ssl_options: Option<mqtt::SslOptions>,
}

pub struct UPClientMqtt {
    mqtt_client: Arc<mqtt::AsyncClient>,
}

#[allow(dead_code)]
impl UPClientMqtt {
    pub async fn new(config: MqttConfig, client_id: UUID) -> Result<Self, UStatus> {
        let mqtt_protocol = if config.ssl_options.is_some() {
            "mqtts"
        } else {
            "mqtt"
        };

        let mqtt_uri = format!(
            "{}://{}:{}",
            mqtt_protocol, config.mqtt_namespace, config.mqtt_port
        );

        let mqtt_cli = mqtt::CreateOptionsBuilder::new()
            .server_uri(mqtt_uri)
            .client_id(client_id)
            .mqtt_version(MQTT_VERSION_5)
            .max_buffered_messages(100)
            .create_client()
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Unable to create mqtt client: {e:?}"),
                )
            })?;

        // TODO: Integrate ssl options when connecting, may need a username, etc.
        let conn_opts = mqtt::ConnectOptionsBuilder::new_v5()
            .clean_session(true)
            .finalize();

        mqtt_cli.connect(conn_opts).await.map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to connect to mqtt broker: {e:?}"),
            )
        })?;

        Ok(Self {
            mqtt_client: Arc::new(mqtt_cli),
        })
    }

    fn get_client_id(&self) -> String {
        self.mqtt_client.client_id()
    }

    // Serialize UMessage for transport over mqtt
    fn serialize_umessage(message: &UMessage) -> Result<Vec<u8>, UStatus> {
        let bytes = message.write_to_bytes().map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to serialize uMessage: {e:?}"),
            )
        })?;

        Ok(bytes)
    }

    fn deserialize_umessage(bytes: &[u8]) -> Result<UMessage, UStatus> {
        let message = Message::parse_from_bytes(bytes).map_err(|e| {
            UStatus::fail_with_code(
                UCode::INTERNAL,
                format!("Unable to deserialize uMessage: {e:?}"),
            )
        })?;

        Ok(message)
    }

    fn mqtt_topic_from_uuri(uri: &UUri) -> Result<String, UStatus> {
        //TODO: determine how to best split up the uri for an mqtt topic.
        // This is a placeholder implementation.

        Ok(uri.to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_serialize_umessage_and_back() {
        let message = UMessage {
            ..Default::default()
        };

        let bytes = UPClientMqtt::serialize_umessage(&message).unwrap();
        let message_from_bytes = UPClientMqtt::deserialize_umessage(&bytes).unwrap();
        assert_eq!(message, message_from_bytes);
    }
}
