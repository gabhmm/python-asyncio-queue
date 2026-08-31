import asyncio
import logging
from message_payload import ActionType, MessagePayload

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

logger = logging.getLogger(__name__)
 
async def produtor( 
    nome: str,
    acoes: list[tuple[ActionType, str]],
    fila: asyncio.Queue[MessagePayload | None],
) -> None:
 
    logger.info(f"[Produtor {nome}] Iniciando geração de mensagens...")

    for action, content in acoes:

        payload: MessagePayload = MessagePayload.create(nome,action,content)

        logger.info(f"[Produtor {nome}] Enfileirando -> {action}: {content}")

        await asyncio.sleep(0.5)
        await fila.put(payload)
    logger.info(f"[Produtor {nome}] Finalizou a produção de mensagens.")
    

async def consumidor(
    worker_id: int,
    fila: asyncio.Queue[MessagePayload | None],
) -> None:
    """Consome e processa mensagens MessagePayload retiradas da fila."""


async def main() -> None:
    """Coordena a fila, produtores e consumidores com concorrência estruturada."""


if __name__ == "__main__":
    asyncio.run(main())
