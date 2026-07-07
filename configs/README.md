# Trino Server Configuration

## Tổng quan

Thư mục `configs/` chứa toàn bộ cấu hình cho **Trino query engine** — thành phần đảm nhiệm vai trò SQL analytics layer trong kiến trúc Lakehouse. Trino hoạt động ở chế độ **single-node coordinator** (phiên bản `trinodb/trino:457`), lắng nghe trên **port 8080**, và kết nối đến Apache Iceberg thông qua REST Catalog để truy vấn dữ liệu trên MinIO (S3-compatible storage).

Các file cấu hình được **bind-mount** vào container Trino thông qua `docker-compose.yml`.

## Cấu trúc thư mục

```
configs/
└── trino/
    └── etc/
        ├── config.properties          # Cấu hình coordinator
        ├── jvm.config                 # Tham số JVM
        ├── log.properties             # Mức độ logging
        ├── node.properties            # Thông tin node
        └── catalog/
            └── .properties     # Iceberg connector catalog
```

## Chi tiết từng file

| File | Mô tả |
|------|--------|
| `config.properties` | Cấu hình Trino coordinator: single-node mode (`node-scheduler.include-coordinator=true`), HTTP port **8080**, max memory **2GB**, discovery URI tự trỏ về localhost |
| `jvm.config` | Cấu hình JVM: heap tối đa **2G** (`-Xmx2G`), sử dụng **G1GC** garbage collector, bật HeapDump và ExitOnOutOfMemoryError để xử lý lỗi bộ nhớ |
| `log.properties` | Thiết lập log level cho Trino server |
| `node.properties` | Định danh node: environment `onehouse`, node ID `onehouse-trino-coordinator`, data directory `/data/trino` |
| `.properties` | **Iceberg connector** — tên file quyết định tên catalog trong Trino (catalog = ``) |

## Cấu hình Iceberg Connector (`.properties`)

```properties
connector.name=iceberg
iceberg.catalog.type=rest
iceberg.rest-catalog.uri=http://iceberg-rest:8181
iceberg.rest-catalog.warehouse=s3://warehouse/

fs.s3.enabled=true
s3.endpoint=http://minio:9000
s3.region=us-east-1
s3.path-style-access=true
```

**Giải thích:**
- **REST Catalog**: kết nối đến Iceberg REST Catalog service tại `iceberg-rest:8181`
- **S3 filesystem**: sử dụng MinIO làm object storage tại `minio:9000` với path-style access
- **Warehouse**: dữ liệu Iceberg table được lưu tại bucket `s3://warehouse/`

## Kết nối với các thành phần khác

```
MinIO (Object Storage) ←── S3 protocol ──→ Trino ←── REST API ──→ Iceberg REST Catalog
                                              ↑
                                         dbt / Superset
                                       (query qua port 8080)
```

- **dbt**: kết nối đến Trino tại `trino:8080` để thực thi transformation (catalog ``)
- **Superset**: kết nối đến Trino để trực quan hóa dữ liệu từ các bảng gold layer
- **Iceberg REST Catalog**: quản lý metadata của các Iceberg table
- **MinIO**: lưu trữ Parquet data files của Iceberg

## Truy cập

| Giao diện | URL |
|-----------|-----|
| Trino Web UI | `http://localhost:8080` |
| Trino CLI (trong container) | `docker exec -it onehouse-trino trino --catalog ` |
