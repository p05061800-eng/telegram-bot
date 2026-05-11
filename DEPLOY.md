# IlluCards Telegram Bot — гид и деплой

## Локальный запуск

1. **Python 3.11+** (как в `Dockerfile`).
2. Виртуальное окружение и зависимости:

```bash
cd /path/to/bot
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

3. Файл **`.env`** в корне (или переменные в панели хостинга). Минимум:

| Переменная | Обязательно | Описание |
|------------|-------------|----------|
| `TELEGRAM_BOT_TOKEN` | да | Токен от [@BotFather](https://t.me/BotFather) |

4. Запуск:

```bash
python3 main.py
```

Точка входа для хостингов: `main.py` → `bot.main()`. Можно также `python3 bot.py`.

После старта в логе появится ссылка `https://t.me/<username>`, если у бота задан username.

---

## Что работает в одном процессе

| Компонент | Назначение |
|-----------|------------|
| **python-telegram-bot** | Long polling (`run_polling`) |
| **Flask** на `PORT` | `GET/HEAD /`, `GET /health` — healthcheck Render / UptimeRobot (по умолчанию порт `10000`, если `PORT` не задан) |
| **aiohttp** | Вход с сайта и синк: `/`, `/login`, `POST /api/send-code`, `/api/verify-code`, `/api/telegram-auth`, `POST /api/sync/cart`, `…/favorites`, `…/state`, `…/promotions` |

По умолчанию aiohttp слушает **`127.0.0.1:8765`** (`LOGIN_API_PORT`), чтобы не занимать тот же сокет, что Flask на `PORT`.

### Важно для продакшена (сайт ↔ бот)

- С **Vercel / браузера** запросы должны попадать на **публичный URL**, где реально доступны маршруты `/api/send-code`, `/api/verify-code`, `/api/sync/*`.
- Задайте **`LOGIN_API_PUBLIC_URL`** = тот базовый URL (со схемой), с которого сайт вызывает API (используется в CORS и подстановке в `web/login.html`).
- Если API должно слушать **все интерфейсы**: **`LOGIN_API_HOST=0.0.0.0`**. На **Render Web Service** наружу обычно открыт **один** порт (`PORT` под Flask). Второй порт (8765) снаружи часто **недоступен** — тогда либо отдельный сервис/прокси под логин, либо доработка: вынести маршруты aiohttp за один публичный порт (отдельная задача в коде).
- Отключить HTTP API входа: **`LOGIN_API_DISABLE=1`**.

---

## Переменные окружения (справочник)

| Переменная | Назначение |
|------------|------------|
| `TELEGRAM_BOT_TOKEN` | Токен бота |
| `PORT` | Порт Flask (Render подставляет) |
| `ILLUCARDS_SITE_ORIGIN` | База сайта, напр. `https://www.illucards.by` |
| `ILLUCARDS_ORDER_UPDATE_SECRET` | Секрет как на Vercel для `GET /api/order/{id}` и `POST /api/order/update` |
| `TELEGRAM_SYNC_API_SECRET` | Секрет для `POST /api/sync/*` с сайта в бот |
| `CARDS_JSON_URL` | Каталог JSON; иначе `{ILLUCARDS_BASE}/api/products` |
| `HOME_PROMOTIONS_JSON_URL` | Опционально: JSON баннеров главной для «Акции» |
| `HOME_PROMOTIONS_SCRAPE_DISABLE` | `1` — не подгружать баннеры скрейпом HTML |
| `LOGIN_API_DISABLE` | `1` — не поднимать aiohttp login API |
| `LOGIN_API_PUBLIC_URL` | Публичный базовый URL для login/sync |
| `LOGIN_API_HOST` / `LOGIN_API_PORT` | Хост/порт aiohttp (по умолчанию `127.0.0.1` / `8765`) |
| `POST_LOGIN_REDIRECT` | URL после успешного входа (например `https://www.illucards.by/account`) |
| `TELEGRAM_ORDER_NOTIFY_ID` | Чат уведомлений о заказах (число или `@username`) |
| `ORDER_MENTION` | Упоминание в текстах заказа |
| `TELEGRAM_POLLING_BOOTSTRAP_RETRIES` | Повторы bootstrap при старте polling (по умолчанию 35) |
| `ILLUCARDS_CART_CLEAR_ON_PROOF_URL` | Опционально: URL для **POST**, когда клиент нажал «✅ Оплатить» в боте — очистить корзину на сайте (см. «Корзина и чек») |
| `ILLUCARDS_LOYALTY_EARN_PERCENT` | `0`…`100` — оценка начисляемых бонусов с заказа, если нет ни явных полей сайта, ни правила ниже (по умолчанию `5` = 5% от суммы к оплате) |
| `ILLUCARDS_LOYALTY_POINTS_PER_UNIT` | Баллов за 1 ед. товара, как на витрине (по умолчанию `100`). Если `0`, правило отключено — тогда остаётся процент. Оценка = сумма количеств по позициям × это число |

`UPSTASH_REDIS_REST_URL` и `UPSTASH_REDIS_REST_TOKEN` используются ботом для долговременного сохранения заказов, корзин, избранного, бонусов и служебных состояний между деплоями. Если Redis не задан, бот использует локальный `bot_state.json`, который на Render без persistent disk может пропадать после деплоя.

### Бонусы и сайт

Баланс и начисления приходят из JSON синка / verify-code (поля вроде `bonusPoints`, `bonusBalance`, `bonusEarned` — см. `_parse_site_loyalty_snapshot` в `bot.py`). В боте: кнопка **«⭐ Бонусы»**, списание к оплате в превью заказа (кнопки «Выкл» / «50%» / «Макс»). Фактическое списание на стороне сайта — ваша интеграция после оплаты / sync.

Для строки в оплате «⭐ Ориентировочно начислится с заказа: ~N» порядок такой: (1) явное число в payload сайта (`bonusWillEarn`, `bonusForOrder`, …); (2) если в строках корзины пришли числа по позициям — их сумма; (3) **количество единиц товаров** × `ILLUCARDS_LOYALTY_POINTS_PER_UNIT` (по умолчанию 100 — как «100 баллов за единицу» на витрине); (4) иначе процент `ILLUCARDS_LOYALTY_EARN_PERCENT`. Если сайт не шлёт явное поле, но показывает ~300 за 3 товара, бот после деплоя совпадёт с этим правилом без доп. полей в JSON.

### Акции главной

Источники: `POST /api/sync/promotions`, JSON в полном sync, опционально `HOME_PROMOTIONS_JSON_URL`. Fallback — парсинг слайдера витрины (`swiper-slide` + `aspect-video` до `#collection`) на главной сайта.

### Избранное (сайт → бот)

Кнопка **«Избранное»** в Telegram показывает только **`USER_FAVORITES`**, который бот получает из API, а не напрямую с витрины.

- Отдельно: **`POST /api/sync/favorites`** — тело JSON со списком в одном из полей: `items`, `favorites`, `favoriteItems`, `wishlist`, `favoriteIds` и др. (см. `_FAVORITE_SYNC_PAYLOAD_KEYS` в `bot.py`). Элементы — строки `id` или объекты с `id` / `productId` / `name` / `sku` / `slug` и т.п.; **id должен совпадать с полем `id` карточки в каталоге** (`CARDS_JSON_URL`/products API), иначе позиция не отобразится.
- При **`POST /api/sync/state`** или **`POST /api/sync/cart`** тот же список можно передать любым поддерживаемым ключом (**не используйте** ключ **`items`** для избранного в этих методах — в `sync/cart` **`items`** зарезервировано под **корзину**).
- При **`POST /api/verify-code`** (после входа с сайта) избранное подхватится теми же ключами.

Если на сайте «сердца» есть, а в боте пусто — нужно добавить вызов одного из endpoint’ов с `telegramUserId` / секретом `TELEGRAM_SYNC_API_SECRET` и массивом избранного.

### Корзина и чек

- **В боте** позиции корзины и подсказки цен с сайта (`site_cart_grand_*`) сбрасываются **после нажатия клиентом «✅ Оплатить»** (callback `paid`), когда начинается шаг со скрином оплаты — **не** сразу после «Подтвердить заказ» (`ta:0` / превью). Черновик оформления (`order_checkout`, страна доставки и т.д.) тоже очищается в этот момент. Нажатие админом «Принять заказ» корзину не трогает. Подтверждение оплаты админом — списание бонусов и чек.
- **На сайте** бот сам корзину не чистит: нужен ваш обработчик. Если задан **`ILLUCARDS_CART_CLEAR_ON_PROOF_URL`**, в **тот же момент** (после «Оплатить» в боте) выполняется **POST** с заголовком **`Authorization: Bearer …`** (тот же секрет, что **`ILLUCARDS_ORDER_UPDATE_SECRET`**, если задан) и JSON-телом:

```json
{
  "telegramUserId": 123456789,
  "botOrderId": 42,
  "externalOrderId": "uuid-с-сайта-или-null",
  "event": "payment_pay_clicked"
}
```

Реализуйте на стороне сайта очистку корзины пользователя по `telegramUserId` (и при необходимости свяжите с заказом через `externalOrderId`). Если переменная не задана, запрос не отправляется.

---

## Docker

```bash
docker build -t illucards-bot .
docker run --rm \
  -e TELEGRAM_BOT_TOKEN="..." \
  -e PORT=8080 \
  -p 8080:8080 \
  illucards-bot
```

`CMD`: `python3 main.py`. В образ копируются `bot.py`, `main.py`, `web/`, `requirements.txt`.

---

## Render

В репозитории: **`render.yaml`** — сервис `illucards-telegram-bot`, тип **web**, **Docker**, `autoDeploy: true`.

1. Подключите репозиторий к [Render](https://render.com), создайте **Web Service** из Dockerfile / Blueprint.
2. Задайте **Environment** (секреты вручную, т.к. `sync: false`):
   - `TELEGRAM_BOT_TOKEN`
   - `ILLUCARDS_SITE_ORIGIN`
   - `ILLUCARDS_ORDER_UPDATE_SECRET`
   - при необходимости `TELEGRAM_SYNC_API_SECRET`, `LOGIN_API_PUBLIC_URL`, `POST_LOGIN_REDIRECT`, и т.д.
3. После **push в подключённую ветку** Render сам пересоберёт и задеплоит (`autoDeploy`).

Health: **GET/HEAD** на корень и **`/health`** на `PORT`.

---

## После деплоя

1. В [@BotFather](https://t.me/BotFather) не держите **webhook**, если бот на polling (при старте webhook снимается).
2. Проверьте: каталог, корзина, оформление, оплата, уведомления в `TELEGRAM_ORDER_NOTIFY_ID`.
3. Проверьте вызовы с сайта на URL логина/sync согласно `LOGIN_API_PUBLIC_URL` и доступности порта.

**Сессия в памяти:** в `post_init` сбрасываются `LOGIN_CODES` и глобальный `SITE_LOGIN_PENDING_ORDER` (на чистом старте процесса словари пустые). При **первом** действии пользователя в чате (`deploy_epoch`) сбрасываются черновик оформления в `user_data`, tinder-задачи, reply-режимы и т.п. **Корзина `USER_CART` и черновик «с сайта» в памяти не сбрасываются** при этом — иначе после `verify-code` / sync первое нажатие «💚 Корзина» показывало бы пусто. **Старые сообщения в чате** Telegram не удаляет; кнопки могут отвечать «устарело».

---

## Частые проблемы

| Симптом | Что проверить |
|---------|----------------|
| 401 к API заказа с сайта | `ILLUCARDS_ORDER_UPDATE_SECRET` совпадает с Vercel |
| Нет баннеров «Акции» | `POST /api/sync/promotions`, `HOME_PROMOTIONS_JSON_URL` или доступность главной для скрейпа |
| Нет бонусов в боте | В JSON sync/verify-code есть поля баланса; вход по коду с сайта |
| Сайт не достучится до send-code | `LOGIN_API_PUBLIC_URL`, хост/порт aiohttp, один порт на Render |
