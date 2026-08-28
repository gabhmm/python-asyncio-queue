import asyncio
import logging
from message_payload import ActionType, MessagePayload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

async def produtor(
    nome: str,
    acoes: list[tuple[ActionType, str]],
    fila: asyncio.Queue[MessagePayload | None],
) -> None:
    """Produz mensagens MessagePayload e as insere na fila assíncrona."""


async def consumidor(
    worker_id: int,
    fila: asyncio.Queue[MessagePayload | None],
) -> None:
    """Consome e processa mensagens MessagePayload retiradas da fila."""


async def main() -> None:
    """Coordena a fila, produtores e consumidores com concorrência estruturada."""


if __name__ == "__main__":
    asyncio.run(main())
