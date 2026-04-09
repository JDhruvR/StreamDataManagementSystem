from streaming.kafka_config import KafkaConfig, set_default_config, get_default_config


def test_kafka_config_is_always_ephemeral():
    cfg = KafkaConfig()
    topic_cfg = cfg.get_topic_config()

    assert cfg.is_persistent() is False
    assert topic_cfg["log.retention.ms"] == 1
    assert "ephemeral" in cfg.get_mode_description().lower()


def test_set_default_config_keeps_ephemeral_mode():
    set_default_config(broker="localhost:9092")
    cfg = get_default_config()
    assert cfg.is_persistent() is False
    assert cfg.get_topic_config()["log.retention.ms"] == 1
