from telethon import utils
from telethon.tl.types import (
    User, Channel, Chat,
    PeerUser, PeerChannel, PeerChat,
    InputPeerUser, InputPeerChannel, InputPeerChat,
)


def canonical(entity_or_peer_or_id) -> int:
    v = entity_or_peer_or_id
    if isinstance(v, int):
        return _canonical_int(v)
    return utils.get_peer_id(v)


def _canonical_int(v: int) -> int:
    if v > 0:
        return v
    s = str(v)
    if s.startswith("-100"):
        return v
    return v


def peer_kind(peer) -> str:
    if isinstance(peer, (PeerUser, InputPeerUser, User)):
        return "user"
    if isinstance(peer, (PeerChannel, InputPeerChannel)):
        return "channel"
    if isinstance(peer, Channel):
        return "channel" if peer.broadcast else "supergroup"
    if isinstance(peer, (PeerChat, InputPeerChat, Chat)):
        return "group"
    return "unknown"


def is_user_peer(peer) -> bool:
    return isinstance(peer, (PeerUser, InputPeerUser, User))


def is_channel_peer(peer) -> bool:
    if isinstance(peer, (PeerChannel, InputPeerChannel)):
        return True
    if isinstance(peer, Channel):
        return True
    return False
