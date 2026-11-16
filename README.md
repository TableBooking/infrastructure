# RabbitMQ Infrastructure

Broker dùng chung cho Booking, Operations, Tracking và Notification theo hợp đồng V2. Chạy độc lập khi chưa có mã nguồn các service khác. Phạm vi repository hiện tại gồm RabbitMQ/topology/tài khoản và các kiểm tra; chưa triển khai Gateway, Auth hay PostgreSQL dùng chung cho toàn hệ thống.

## Khởi động

Yêu cầu Docker Engine/Desktop + Compose v2 có lệnh `wait`, Node.js 22.19+. Chạy từ thư mục này:

```powershell
npm run up
npm run check
```

`npm run up` tự tạo `.env` từ [.env.example](./.env.example) nếu chưa có, sinh mật khẩu ngẫu nhiên độc lập cho 5 tài khoản, giữ nguyên credentials đã cấu hình, khởi động broker rồi **đợi bootstrap kết thúc thành công**. Các file kết nối của service được ghi vào [generated/](./generated/) và được Git bỏ qua. Không cần cài dependencies npm để khởi động broker; `npm ci` chỉ cần cho bộ test AMQP.

- Management UI: `http://127.0.0.1:15672`; dùng `RABBITMQ_ADMIN_USER` / `RABBITMQ_ADMIN_PASSWORD` trong `.env`.
- AMQP từ host: `127.0.0.1:5672`; vhost mặc định `restaurant`.
- Container trong network `restaurant-backend`: `rabbitmq:5672`.
- Cổng AMQP/management của overlay dev chỉ bind localhost. [compose.yaml](./compose.yaml) gốc không publish cổng nào; [compose.dev.yaml](./compose.dev.yaml) bổ sung truy cập local.
- Broker dùng named volume, hostname/nodename cố định, healthcheck trạng thái/alarms, tự restart và giới hạn dung lượng Docker logs. Image RabbitMQ 4.4.0 và image bootstrap Node được pin digest đã kiểm tra.

Có thể sửa port, vhost, network và tên tài khoản trong `.env` trước khi chạy. Khi đổi port cần bảo đảm không trùng service khác. Mỗi lần chạy setup/up tạo lại file kết nối theo cấu hình hiện tại.

## Bootstrap và topology

[rabbitmq/topology.json](./rabbitmq/topology.json) là cấu hình khai báo. Job `rabbitmq-init` đợi broker healthy, import topology, tạo/cập nhật tài khoản service, gán resource/topic permissions và kiểm tra kết quả. Chạy lại không purge hoặc xóa queue/message. Khi phát hiện khác biệt thuộc tính cố định của resource hiện có, job dừng và báo tên resource; cần migration có kiểm soát thay vì reset volume.

| Exchange | Loại | Thuộc tính |
| --- | --- | --- |
| `restaurant.events` | topic | durable |
| `restaurant.retry` | direct | durable |
| `restaurant.dlx` | direct | durable |

| Queue chính | Routing keys từ `restaurant.events` |
| --- | --- |
| `operations.requests.q` | `booking.requested.v1`, `booking.cancel-requested.v1` |
| `booking.lifecycle.q` | `booking.state-changed.v1`, `booking.cancellation-rejected.v1` |
| `tracking.lifecycle.q` | `booking.state-changed.v1` |
| `notification.lifecycle.q` | `booking.state-changed.v1` |

Mỗi queue chính có hai queue đi kèm: `<queue>.retry` và `<queue>.dlq`; tổng cộng **12 queue durable classic và 14 binding**. Queue được provision ngay cả khi consumer chưa chạy, nên Tracking và Notification giữ các bản sao riêng và tích backlog độc lập.

- Main queue có `x-dead-letter-exchange=restaurant.dlx`, routing key bằng tên queue chính.
- Retry queue bind `restaurant.retry` với routing key bằng tên queue chính. TTL **5000 ms**, dead-letter qua default exchange `""` trở lại đúng queue chính.
- DLQ bind `restaurant.dlx` với routing key bằng tên queue chính, không tự hết hạn.
- Consumer chọn retry có backoff hay đưa vào DLQ bằng confirmed republish; infrastructure không tự đếm số lần retry cho ứng dụng. Booking dùng `x-booking-retry-count`, mặc định 3 lần; các consumer khác cần triển khai tương tự với giới hạn hữu hạn.
- Các service phải khai báo đúng exchange type, durable, queue arguments và TTL này. Không đổi riêng TTL phía service nếu topology chung chưa được migration.

Producer cần persistent messages, mandatory routing và publisher confirms; consumer cần inbox/transaction cùng thay đổi nghiệp vụ, ACK sau commit. Infrastructure cung cấp transport, queue/retry/DLQ; việc chống trùng, chống lùi version và quyết định booking vẫn thuộc các service. Queue classic trên một broker là mô hình cho đồ án, chưa có HA; không cam kết exactly-once hoặc ordering toàn hệ thống.

## Tài khoản và kết nối service

| Role | Quyền publish domain event | Queue được consume |
| --- | --- | --- |
| Booking | requested, cancel-requested | `booking.lifecycle.q` |
| Operations | state-changed, cancellation-rejected | `operations.requests.q` |
| Tracking | Không publish domain event | `tracking.lifecycle.q` |
| Notification | Không publish domain event | `notification.lifecycle.q` |

Mỗi role có mật khẩu riêng, quyền quản lý/read queue của chính mình, khai báo các exchange chung và publish retry/DLQ. Topic permissions giới hạn routing key của `restaurant.events`; không cấp publish qua default exchange để tránh gửi thẳng vào queue của service khác. Chỉ tài khoản admin có quyền quản trị Management UI. Auth không cần tài khoản RabbitMQ cho MVP.

Sau `npm run setup`/`npm run up`, mỗi role có hai file credentials riêng:

```text
generated/booking.host.env
generated/booking.docker.env
generated/operations.host.env
generated/operations.docker.env
generated/tracking.host.env
generated/tracking.docker.env
generated/notification.host.env
generated/notification.docker.env
```

Các file chứa `RABBITMQ_URL` đã encode username/password/vhost, `RABBIT_RETRY_DELAY_MS=5000` và tên network. Chỉ phân phối file của đúng service qua kênh quản lý secrets của nhóm. File `.env`, thư mục generated và node_modules được Git ignore; setup không in mật khẩu/URL có credentials.

Booking hiện có thể kết nối ngay; xem [hướng dẫn Booking dùng broker chung](../booking-service/README.md#broker-dùng-chung-từ-infrastructure). Với service Docker mới, gán `env_file` tới file `*.docker.env` của service và tham gia external network:

```yaml
services:
  operations:
    # image/build của Operations do service owner cung cấp
    env_file: ./generated/operations.docker.env
    networks: [backend]
networks:
  backend:
    external: true
    name: restaurant-backend
```

Nếu đặt tất cả service trong cùng Compose, dùng `depends_on: rabbitmq-init: {condition: service_completed_successfully}` trước khi nhận traffic. `depends_on` không thay thế reconnect trong service. Nếu dùng Compose project riêng, khởi động infrastructure bằng `npm run up` trước, rồi mới khởi động service.

## Vận hành

```powershell
# Kiểm tra cả topology, không chỉ process broker
npm run check
docker compose -f compose.yaml -f compose.dev.yaml ps -a
docker compose -f compose.yaml -f compose.dev.yaml logs --tail=100 rabbitmq rabbitmq-init

# Dừng, giữ nguyên volume và backlog
docker compose -f compose.yaml -f compose.dev.yaml stop

# Khởi động lại và áp dụng cấu hình idempotently
npm run up
```

Mật khẩu/tên admin từ `RABBITMQ_DEFAULT_*` chỉ seed khi volume mới. Đổi admin password trong `.env` không tự đổi password đã lưu trong broker: cập nhật qua Management UI với tài khoản hiện tại rồi đồng bộ `.env`. Đổi mật khẩu service trong `.env` và chạy lại `npm run up` để áp dụng; restart/reconnect service sau khi nhận file credentials mới. Không xóa volume để xử lý lỗi đăng nhập.

DLQ giữ nguyên payload/eventId để người vận hành điều tra. Sau khi sửa nguyên nhân, replay có kiểm soát về đúng queue của consumer và giữ nguyên `eventId` trong JSON để inbox chống trùng; chưa cung cấp công cụ replay tự động. Definitions import là additive, không tự xóa resource/user cũ khi đổi tên cấu hình. Việc xóa queue/user hoặc đổi queue type cần xem backlog và migration riêng.

## Kiểm tra

```powershell
npm ci
npm test
npm run up
npm run check
npm run test:integration
```

Unit tests kiểm tra setup giữ nguyên secrets, validate cấu hình, encode URL, topology, quyền và từ chối resource không tương thích. Integration tests dùng broker thật, tạo vhost/user tạm và dọn sau test; không purge queue ứng dụng. Kiểm tra publish/consume bằng account đúng role, fanout, consumer offline, mandatory return, access denied, retry TTL, DLQ và `x-death` headers.

Kiểm tra persistence riêng bằng lệnh sau; **lệnh này restart broker của Compose project này**, làm gián đoạn ngắn các service đang kết nối:

```powershell
npm run test:restart
```

Test gửi message persistent vào vhost riêng, restart broker, rồi kiểm tra message, queue và credentials vẫn tồn tại, không chạy bootstrap lại giữa hai bước. Chưa kiểm thử lifecycle nghiệp vụ đầy đủ của các service chưa có code hoặc triển khai lên VPS.

Tham chiếu triển khai: [RabbitMQ definitions](https://www.rabbitmq.com/docs/definitions), [resource và topic permissions](https://www.rabbitmq.com/docs/access-control).
