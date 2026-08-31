import asyncio
import pytest
from message_payload import ActionType, MessagePayload
from main import produtor


def test_message_payload_empty_field():
    with pytest.raises(ValueError):
        MessagePayload.create("", "AUTH", "SELECT * FROM tb_clientes")


def test_caracter_incorreto():
    with pytest.raises(ValueError):
        MessagePayload.create("|", "AUTH", "SELECT * FROM tb_clientes")


def test_message_payload_caminho_feliz():
    mp = MessagePayload.create("cliente01", "AUTH", "token_123")
    assert isinstance(mp, MessagePayload)

def test_action_incorreta():
    with pytest.raises(ValueError):
        MessagePayload.create("|", "LOGOUT", "SELECT * FROM tb_clientes")

@pytest.mark.asyncio
async def test_produtor_enfileira_itens_corretos():
    fila: asyncio.Queue[MessagePayload | None] = asyncio.Queue()
    acoes: list[tuple[ActionType, str]] = [("AUTH", "token_123")]

    await produtor("Sensor-Teste", acoes, fila)

    item = await fila.get()
    assert item.client_id == "Sensor-Teste"
    assert item.action == "AUTH"
    assert item.content == "token_123"