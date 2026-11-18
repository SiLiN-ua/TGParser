from telethon.tl.types import (
    Channel, Chat, User, PeerUser, PeerChannel, PeerChat,
)
from tgparser.ids import canonical, is_user_peer, is_channel_peer


def _mk_channel(cid, broadcast=True):
    return Channel(
        id=cid, title="t", photo=None, date=None,
        broadcast=broadcast, megagroup=not broadcast,
        verified=False, min=False, restricted=False,
        signatures=False, gigagroup=False, scam=False,
        has_link=False, has_geo=False, slowmode_enabled=False,
        call_active=False, call_not_empty=False, fake=False,
        noforwards=False, join_to_send=False, join_request=False,
        forum=False, stories_hidden=False, stories_hidden_min=False,
        stories_unavailable=False,
        access_hash=0, username=None,
    )


def test_user_id_stable():
    assert canonical(123) == 123
    assert canonical(PeerUser(user_id=42)) == 42


def test_channel_id_becomes_marked():
    c = _mk_channel(1006503122, broadcast=True)
    assert canonical(c) == -1001006503122
    assert canonical(PeerChannel(channel_id=1006503122)) == -1001006503122


def test_group_id_becomes_negative():
    assert canonical(PeerChat(chat_id=12345)) == -12345


def test_canonical_int_noop_when_already_marked():
    assert canonical(-1001006503122) == -1001006503122


def test_peer_kind_helpers():
    assert is_user_peer(PeerUser(1))
    assert not is_user_peer(PeerChannel(1))
    assert is_channel_peer(PeerChannel(1))
    assert not is_channel_peer(PeerUser(1))
