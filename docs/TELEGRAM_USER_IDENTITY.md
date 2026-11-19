# Telegram user identity — что доказуемо, что нет

Этот документ фиксирует, как Telegram представляет пользователя и какие
гарантии даёт Telethon — только на основе официальных источников. Ссылки в
конце.

## TL-ссылка на `User`

Объект `User` в MTProto (ссылка [1][2]) содержит:

- `id: long` — числовой идентификатор аккаунта;
- `access_hash: long` *(nullable)* — криптографический токен, который
  требует Telegram, чтобы делать запросы, адресованные *этому* пользователю
  *из вашего аккаунта*; не постоянный, выдаётся вам сервером в контексте
  определённых событий;
- `first_name`, `last_name`, `username`, `phone` — атрибуты профиля;
- флаги `bot`, `premium`, `verified`, `scam`, `fake`, `restricted`,
  `deleted`, `min`, `mutual_contact`, `contact`, `close_friend`, …;
- `photo: UserProfilePhoto` — текущая аватарка (с собственным `photo_id`).

`UserEmpty` — заглушка: только `id`.

Поле `min=true` ([3]) означает, что сервер прислал объект «минимального»
качества: часть полей отсутствует, а `access_hash`, если и есть, пригоден
только для ограниченного набора запросов, и его **нельзя** сохранять
долгоживущим токеном.

`UserFull` ([4]) — результат `users.getFullUser` — добавляет `about` (bio),
`settings`, настройки приватности и прочие расширенные данные.

## Ответы на вопросы

### 1. Является ли `user.id` постоянным уникальным идентификатором аккаунта?

**Да.** `user.id` (int64) выдаётся при создании аккаунта и не меняется в
течение жизни аккаунта (см. Telethon ref [1] и общие замечания [5]).
Удалённый аккаунт обычно сохраняет свой id с флагом `deleted=true`. Новый
аккаунт с тем же номером телефона получает **другой** `user.id`.

### 2. Можно ли по сообщению всегда определить настоящий аккаунт автора?

**Нет.** В супергруппе/канале возможны случаи:

- обычное сообщение от пользователя: `message.from_id = PeerUser(user_id)` —
  автора определить можно;
- сообщение опубликовано от имени канала (анонимный админ, broadcast):
  `from_id = PeerChannel(channel_id)` — настоящий человек-автор **скрыт**
  протоколом, доступно только имя, если его указал админ
  (`message.post_author`);
- forward от имени «скрытого» отправителя: `fwd_from.from_name` — только
  строка имени, без `from_id`;
- forward от пользователя, у которого включена privacy «показывать
  источник как имя»: аналогично, только имя.

### 3. Anonymous admin / сообщение от канала

Хранится как `sender_chat_id = канал/супергруппа`, `sender_user_id = NULL`.
Выдавать выдуманный `user_id` нельзя.

### 4. Можно ли получить `access_hash` для каждого автора?

**Не всегда.** Telegram возвращает `access_hash` только если в контексте
текущей сессии пользователь «известен» (например, появлялся как отправитель
сообщения в канале/группе, где вы состоите, или вы запрашивали через
`users.getUsers` с полноценным `InputUser`).

`min=true` entities часто приходят без годного `access_hash`. Для таких
пользователей надо получить «полноразмерный» entity: обычно через
`client.get_input_entity(user_id)` в контексте известного сообщения, либо
через `GetUsersRequest` c корректным `InputUser`.

### 5. Стабилен ли `access_hash`? Можно ли считать глобальным идентификатором?

**Нет.** `access_hash` ([6], [7]):

- специфичен для **вашего** аккаунта (у другого пользователя будет другой);
- может быть **пересчитан** Telegram'ом (например, после обнаружения
  утечки);
- в `min=true` entities это ограниченный вариант;
- **НЕ** является идентификатором личности и не должен участвовать в
  первичном ключе.

### 6. Можно ли получить профиль только по числовому ID без `access_hash`?

Прямого MTProto-метода «отдай всё по user_id» нет. Нужен `InputUser` или
`InputPeerUser`, который состоит из `(user_id, access_hash)`. Telethon
подставит `access_hash` из session cache, если он там есть ([8], [9]):

```
await client.get_input_entity(user_id)   # лезет в кэш session
await client.get_entity(user_id)         # то же + сетевой fallback
```

Если cache пуст и сетевой fallback не нашёл — Telethon поднимет
`ValueError: Could not find the input entity for PeerUser(...)`.

### 7. username, bio, phone, avatar

- **username** — публичен, если задан; nullable. Может меняться; старый
  освобождается.
- **bio** (`UserFull.about`) — приватность управляется пользователем;
  может прийти `null`.
- **phone** — приходит только для своих контактов (и даже тогда — по
  privacy settings) или для вашего собственного аккаунта. Для третьих
  лиц обычно `null`.
- **avatar** — публично доступна при наличии, `user.photo.photo_id`
  используется как файл-источник; при изменении `photo_id` меняется.

### 8. Какие поля скрываются privacy settings?

- `phone` — `PRIVACY_PHONE_NUMBER`;
- `about` (bio) — `PRIVACY_ABOUT`;
- `last_seen` / online status — `PRIVACY_STATUS_TIMESTAMP`;
- показывать forward как канала vs как имя — `PRIVACY_FORWARDS`;
- номера, по которым можно найти — `PRIVACY_FIND_BY_PHONE`;
- видимость профильного фото в некоторых случаях.

Отдать privacy-null не является ошибкой коллектора.

### 9. Что доступно только контактам/админам?

- Полный `phone` — только контактам;
- `last_name` в некоторых сценариях скрыт;
- `GetParticipants`/`GetParticipantsRequest` для больших групп — ограничено
  (капы Telegram'а, часто admin-only).

### 10. Что происходит при смене username/имени/аватара?

`user.id` остаётся. Старые `username` освобождаются и могут быть заняты
другим аккаунтом. Поэтому **username нельзя использовать как ключ человека**.
Нужно хранить историю изменений.

### 11. Сопоставить аккаунт между чатами?

Через `user.id` — **да**, это то же лицо. Через username — **нет**.

### 12. Что после удаления аккаунта?

Telegram возвращает минимальные данные (`deleted=true`), `access_hash`
обычно теряется, имя пустое. `user.id` остаётся как historical.

### 13. Что нельзя получить API вообще?

- Реального автора, когда он выбрал подпись каналом/анонимом;
- приватные bio/phone, если это запрещено в privacy settings;
- IP, устройство, email пользователя;
- участников больших приватных групп в полном объёме без админа;
- старые редакции сообщений, которые были до момента, как вы стали их
  читать.

## Канонические правила модели

- Первичный ключ человека → **`user_id` BIGINT**. Не username. Не
  access_hash.
- `access_hash` → техническое поле, обновляется при каждой свежей entity,
  history опционально.
- channel sender сообщения → `sender_chat_id`, не `sender_user_id`.
- Anonymous admin / hidden forward → **не приписывать** несуществующего
  user_id.
- Deleted account → `is_deleted=true`, хранить что есть.
- Один аккаунт в двух чатах → одна строка в `users` + несколько строк в
  `user_observations`.

## Ссылки

1. Telethon — User constructor:
   <https://tl.telethon.dev/constructors/user.html>
2. Telegram core TL — User: <https://core.telegram.org/constructor/user>
3. Telethon FAQ — min users and chats:
   <https://docs.telethon.dev/en/stable/concepts/entities.html#using-input-peers-vs-input-entities>
   (см. раздел «Min entities»)
4. Telethon — UserFull: <https://tl.telethon.dev/constructors/user_full.html>
5. Telegram API — getUsers, getFullUser:
   <https://core.telegram.org/method/users.getUsers>
   <https://core.telegram.org/method/users.getFullUser>
6. Telegram API — InputPeer / InputUser: <https://core.telegram.org/type/InputPeer>
7. Telegram API — Peers:
   <https://core.telegram.org/api/peers> (раздел «Access hash»)
8. Telethon concepts — Entities:
   <https://docs.telethon.dev/en/stable/concepts/entities.html>
9. Telethon — client.get_entity / get_input_entity:
   <https://docs.telethon.dev/en/stable/modules/client.html#telethon.client.users.UserMethods.get_entity>
   <https://docs.telethon.dev/en/stable/modules/client.html#telethon.client.users.UserMethods.get_input_entity>
