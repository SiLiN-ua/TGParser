from telethon.tl.types import (
    ReactionEmoji, ReactionCustomEmoji, PeerUser, PeerChannel, PeerChat,
)


def reaction_key(rk) -> str | None:
    if isinstance(rk, ReactionEmoji):
        return rk.emoticon
    if isinstance(rk, ReactionCustomEmoji):
        return f"custom:{rk.document_id}"
    return None


def reactions_total_and_rows(reactions):
    if reactions is None or not getattr(reactions, "results", None):
        return 0, []
    total = 0
    rows = []
    for i, r in enumerate(reactions.results):
        k = reaction_key(r.reaction)
        if k is None:
            continue
        total += r.count
        rows.append({
            "reaction": k, "count": r.count,
            "chosen_order": getattr(r, "chosen_order", None),
        })
    return total, rows


def peer_id_and_kind(peer):
    if isinstance(peer, PeerUser):
        return peer.user_id, "user"
    if isinstance(peer, PeerChannel):
        return int(f"-100{peer.channel_id}"), "channel"
    if isinstance(peer, PeerChat):
        return -peer.chat_id, "group"
    return None, None
