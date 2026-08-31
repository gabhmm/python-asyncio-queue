import pytest
from message_payload import MessagePayload


def test_message_payload_empty_field():
    with pytest.raises(ValueError):
        MessagePayload.create("", "AUTH", "SELECT * FROM tb_clientes")


def test_caracter_incorreto():
    with pytest.raises(ValueError):
        MessagePayload.create("|", "AUTH", "SELECT * FROM tb_clientes")


def test_message_payload_caminho_feliz():
    mp = MessagePayload.create("cliente01", "AUTH", "token_123")
    assert isinstance(mp, MessagePayload)
