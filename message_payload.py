from dataclasses import dataclass
from typing import Final, Literal, Self, TypeGuard
import time

ActionType = Literal["AUTH", "QUERY", "DISCONNECT"]
VALID_ACTIONS: Final[set[str]] = {"AUTH", "QUERY", "DISCONNECT"}
DELIMITER: Final[str] = "\r\n"
SEPARATOR: Final[str] = "|"


def is_action_type(value: str) -> TypeGuard[ActionType]:
    """TypeGuard que valida em runtime e estreita o tipo estático para ActionType."""
    return value in VALID_ACTIONS


@dataclass(frozen=True, slots=True)
class MessagePayload:
    """Modelo imutável e estritamente tipado para mensagens de protocolo de rede em Python 3.12+."""

    client_id: str
    action: ActionType
    content: str
    timestamp: float

    def __post_init__(self) -> None:
        """Valida invariantes do modelo durante a instanciação."""
        if not self.client_id or SEPARATOR in self.client_id:
            raise ValueError(f"client_id inválido ou contém separador '{SEPARATOR}': {self.client_id!r}")
        if not is_action_type(self.action):
            raise ValueError(f"action inválida: {self.action!r}. Esperado um de: {sorted(VALID_ACTIONS)}")
        if self.timestamp < 0:
            raise ValueError(f"timestamp não pode ser negativo: {self.timestamp}")

    def encode_to_wire(self) -> bytes:
        """Serializa o payload no formato delimitado por CRLF pronto para envio no socket."""
        serialized = f"{self.client_id}{SEPARATOR}{self.action}{SEPARATOR}{self.content}{SEPARATOR}{self.timestamp}{DELIMITER}"
        return serialized.encode("utf-8")

    @classmethod
    def decode_from_wire(cls, data: bytes) -> Self:
        """Desserializa e valida um pacote binário recebido da rede.

        Formato esperado: client_id|action|content|timestamp\r\n
        
        Levanta:
            ValueError: Se os dados estiverem corrompidos, incompletos ou violarem o formato.
        """
        raw_text = data.decode("utf-8")
        if raw_text.endswith(DELIMITER):
            raw_text = raw_text[: -len(DELIMITER)]
        elif raw_text.endswith("\n"):
            raw_text = raw_text.rstrip("\r\n")

        if SEPARATOR not in raw_text:
            raise ValueError("Formato de mensagem inválido: nenhum separador encontrado.")

        left_part, ts_str = raw_text.rsplit(SEPARATOR, maxsplit=1)
        header_and_content = left_part.split(SEPARATOR, maxsplit=2)

        if len(header_and_content) != 3:
            raise ValueError(
                "Formato de mensagem inválido. Esperados cabeçalhos (client_id, action, content) e timestamp."
            )

        client_id, action_str, content = header_and_content

        if not is_action_type(action_str):
            raise ValueError(f"Ação desconhecida recebida: {action_str!r}")

        try:
            timestamp = float(ts_str)
        except ValueError as exc:
            raise ValueError(f"Timestamp numérico inválido: {ts_str!r}") from exc

        return cls(
            client_id=client_id,
            action=action_str,
            content=content,
            timestamp=timestamp,
        )

    @classmethod
    def create(cls, client_id: str, action: ActionType, content: str) -> Self:
        """Factory method auxiliar para criar mensagens com timestamp atual."""
        return cls(
            client_id=client_id,
            action=action,
            content=content,
            timestamp=time.time(),
        )
