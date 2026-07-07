# dbt Connection Profile

## Tổng quan

Thư mục `dbt_profiles/` chứa file `profiles.yml` — cấu hình kết nối giữa **dbt** và **Trino query engine**. File này được **bind-mount** vào container dbt tại đường dẫn `~/.dbt/profiles.yml`, cho phép dbt biết cách kết nối đến Trino mà không cần cấu hình thủ công bên trong container.

## Cấu trúc thư mục

```
dbt_profiles/
└── profiles.yml    # Profile kết nối dbt → Trino
```

## Nội dung cấu hình

```yaml
onehouse:
  target: dev
  outputs:
    dev:
      type: trino
      method: none
      user: "{{ env_var('TRINO_USER', 'admin') }}"
      host: "{{ env_var('TRINO_HOST', 'trino') }}"
      port: "{{ env_var('TRINO_PORT', '8080') | int }}"
      database: 
      schema: analytics
      http_scheme: http
      threads: 4
```

## Chi tiết các tham số

| Tham số | Giá trị | Mô tả |
|---------|---------|--------|
| `type` | `trino` | Sử dụng adapter `dbt-trino` để kết nối |
| `method` | `none` | Xác thực không yêu cầu mật khẩu (phù hợp môi trường dev) |
| `user` | `admin` (mặc định) | Tên người dùng Trino, có thể override qua biến môi trường `TRINO_USER` |
| `host` | `trino` (mặc định) | Hostname của Trino container trong Docker network, override qua `TRINO_HOST` |
| `port` | `8080` (mặc định) | Port HTTP của Trino, override qua `TRINO_PORT` |
| `database` | `` | Tên catalog Iceberg trong Trino (tương ứng file `.properties`) |
| `schema` | `analytics` | Schema mặc định khi dbt tạo model (có thể bị override bởi `dbt_project.yml`) |
| `http_scheme` | `http` | Giao thức kết nối (không dùng TLS trong môi trường dev) |
| `threads` | `4` | Số model được dbt chạy song song |

## Biến môi trường

Profile hỗ trợ **Jinja templating** với `env_var()` để linh hoạt thay đổi cấu hình mà không sửa file:

| Biến môi trường | Mặc định | Mục đích |
|------------------|----------|----------|
| `TRINO_USER` | `admin` | Tên user kết nối Trino |
| `TRINO_HOST` | `trino` | Hostname Trino server |
| `TRINO_PORT` | `8080` | Port Trino server |

Các biến này được khai báo trong file `.env` ở thư mục gốc project.

## Kết nối với các thành phần khác

```
dbt_profiles/profiles.yml ──mount──→ Container dbt (~/.dbt/profiles.yml)
                                           │
                                     dbt_onehouse/ (project)
                                           │
                                     Trino (trino:8080)
                                           │
                                     Catalog:  (Iceberg)
```

- **dbt_onehouse/dbt_project.yml**: Khai báo `profile: onehouse` — trùng khớp với tên profile trong file này
- **configs/trino/etc/catalog/.properties**: Catalog `` mà dbt kết nối đến
- **Docker Compose**: Mount file này vào container dbt để tự động cấu hình kết nối

## Lưu ý

> Tham số `schema: analytics` trong profile là schema **mặc định**. Tuy nhiên, `dbt_project.yml` override schema cho từng layer cụ thể (`staging`, `silver`, `gold`) thông qua macro `generate_schema_name.sql`.
