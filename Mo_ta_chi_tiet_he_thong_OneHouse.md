# TÀI LIỆU KIẾN TRÚC & MÔ TẢ CHI TIẾT HỆ THỐNG ONEHOUSE
## NỀN TẢNG STREAMING LAKEHOUSE GIÁM SÁT TRẢI NGHIỆM KHÁCH HÀNG VÀ LƯU LƯỢNG MẠNG VIỄN THÔNG REAL-TIME

---

## 1. TỔNG QUAN DỰ ÁN (PROJECT OVERVIEW)

* **Tên dự án:** OneHouse
* **Mục tiêu:** Xây dựng nền tảng Streaming Lakehouse giám sát trải nghiệm khách hàng và lưu lượng mạng viễn thông Viettel theo thời gian thực (Near Real-time).
* **Ứng viên thực hiện:** Nguyễn Phong Nhã (MSV: B22DCCN573 - Học viện Công nghệ Bưu chính Viễn thông)
* **Đơn vị chủ trì:** Tổng Công ty Giải pháp Doanh nghiệp Viettel (VTS)
* **Mentor hướng dẫn:** Nguyễn Đức Anh (anhnd178@viettel.com.vn)

### Bài toán Nghiệp vụ
Hệ thống giải quyết bài toán xử lý dữ liệu khổng lồ (Big Data) phát sinh từ hạ tầng mạng viễn thông lõi lõi của Viettel. Nguồn dữ liệu từ các trạm phát sóng (BTS) đổ về liên tục bao gồm log cước (CDR - Call Detail Records), log truy cập Internet di động 4G/5G và dữ liệu trải nghiệm người dùng. Hệ thống giúp đội ngũ kỹ thuật phản ứng nhanh với các "điểm đen" về sóng, tối ưu hóa hạ tầng mạng và nâng cao trải nghiệm khách hàng diện rộng thông qua dữ liệu trực quan sát với thời gian thực.

---
Output: 
Tạo giả lập (Simulator) bắn dữ liệu Log viễn thông thô liên tục
"- Hệ thống vận hành: Pipeline từ Ingestion đến Visualization chạy tự động với độ trễ thấp (Near real-time).
- Quản trị dữ liệu: Toàn bộ logic nghiệp vụ được mã hóa (Code-based) trong dbt, có sơ đồ Lineage và tài liệu tự động.
- Chất lượng dữ liệu: 100% dữ liệu lớp Gold được kiểm tra qua các bộ test tự động của dbt trước khi lên báo cáo.
- Sản phẩm bàn giao:
   + 01 Dashboard giám sát bản đồ nhiệt lưu lượng mạng và lỗi trải nghiệm khách hàng.
   + 01 Repo mã nguồn (dbt project, Airflow DAGs) chuẩn hóa quy trình ELT.
  "

## 2. KIẾN TRÚC TỔNG THỂ HỆ THỐNG (ARCHITECTURE OVERVIEW)

Hệ thống được thiết kế theo mô hình **Modern Data Stack (MDS)** trên nền tảng **Lakehouse Architecture**, tối ưu hóa toàn bộ chu trình dữ liệu từ khâu thu nạp, lưu trữ bền vững cho đến biến đổi in-place và phục vụ phân tích hiệu năng cao.

```
+-------------------------------------------------------------------------------------------------+
|                                    LUỒNG DỮ LIỆU ĐẦU CUỐI (E2E)                                 |
+-------------------------------------------------------------------------------------------------+
|                                                                                                 |
|  [ Trạm BTS ] ---> (Log CDR / Internet) ---> [ Apache Kafka ]                                    |
|                                                     |                                           |
|                                           (Spark Streaming)                                     |
|                                                     v                                           |
|                                        +-------------------------+                              |
|                                        |  MinIO Object Storage   |                              |
|                                        |  (Apache Iceberg Format)|                              |
|                                        +-------------------------+                              |
|                                                     ^                                           |
|                                           (Iceberg REST Catalog)                                |
|                                                     v                                           |
|                                        +-------------------------+                              |
|                                        |  Trino MPP SQL Engine   | <--- [ dbt Transformation ]   |
|                                        +-------------------------+      (Lập lịch qua Airflow)  |
|                                                     |                                           |
|                                            (OLAP Queries)                                       |
|                                                     v                                           |
|                                        +-------------------------+                              |
|                                        |     Apache Superset     |                              |
|                                        |   (Real-time Heatmap)   |                              |
|                                        +-------------------------+                              |
+-------------------------------------------------------------------------------------------------+
```

---

## 3. THÀNH PHẦN CHI TIẾT & DATA STACK SPECS

### 3.1. Tầng Thu nạp Dữ liệu (Data Ingestion Layer)
* **Công nghệ:** Apache Kafka & Apache Spark Streaming.
* **Cơ chế hoạt động:**
    * **Apache Kafka:** Đóng vai trò là Distributed Message Broker, tiếp nhận luồng Log viễn thông (CDR, Internet Traffic) khổng lồ từ các trạm phát sóng đổ về theo thời gian thực dưới dạng chuỗi byte (JSON/Protobuf).
    * **Apache Spark Streaming:** Thực hiện cơ chế micro-batching (chu kỳ từ 15 - 30 giây) để liên tục consume dữ liệu từ các Topic của Kafka. Tại đây, Spark thực hiện các tác vụ xử lý thô ở mức cơ bản bao gồm khử trùng lặp (De-duplication), parse định dạng dữ liệu, cấu trúc hóa bản ghi và thực hiện thao tác **Append-only** trực tiếp xuống tầng lưu trữ.

### 3.2. Tầng Lưu trữ & Quản lý Siêu dữ liệu (Storage & Catalog Layer)
* **Công nghệ:** MinIO Object Storage under Apache Iceberg format & Iceberg REST Catalog.
* **Cơ chế hoạt động:**
    * **MinIO:** Hạ tầng lưu trữ đối tượng (Object Storage) hiệu năng cao, đóng vai trò là "Data Lake" lưu trữ toàn bộ các tệp tin dữ liệu định dạng Parquet.
    * **Apache Iceberg:** Định dạng bảng di sản (Table Format) thế hệ mới, chạy trực tiếp trên nền MinIO nhằm biến Data Lake thành **Lakehouse**. Iceberg cung cấp tính năng **ACID Transactions** (Snapshot Isolation), hỗ trợ **Schema Evolution** (thay đổi cấu trúc log mạng linh hoạt mà không gây sập pipeline), và **Time Travel** (truy vấn dữ liệu tại một thời điểm trong quá khứ).
    * **Iceberg REST Catalog:** Đóng vai trò trung tâm quản lý và đồng bộ toàn bộ siêu dữ liệu (Metadata) ở cấp độ file (File-level mechanism). Cơ chế này giúp loại bỏ hiện tượng nghẽn cổ chai (Metastore Lock) của Hive Metastore cũ kỹ khi có hàng nghìn luồng ghi đồng thời từ Spark.

### 3.3. Tầng Tính toán & Biến đổi Dữ liệu (Compute & Transformation Layer)
* **Công nghệ:** Trino MPP SQL Engine & dbt (Data Build Tool).
* **Cơ chế hoạt động:**
    * **Trino:** Công cụ truy vấn SQL phân tán (Distributed MPP SQL Query Engine), tách biệt hoàn toàn giữa năng lực tính toán (Compute) và lưu trữ (Storage). Trino chịu trách nhiệm thực thi các câu lệnh SQL phân tích phức tạp với tốc độ Sub-second.
    * **dbt (Data Build Tool):** Đóng vai trò định nghĩa toàn bộ logic nghiệp vụ biến đổi dữ liệu (Transformation) bằng mã nguồn SQL sạch (`Code-based`). Quá trình xử lý dữ liệu được dbt đẩy trực tiếp xuống Trino để thực thi ngay tại chỗ (**In-place ELT**), không cần dịch chuyển dữ liệu ra khỏi MinIO.

Dữ liệu được tổ chức chặt chẽ qua mô hình **Medallion Architecture (3 lớp)**:
1.  **Staging (Bronze - Lớp thô):** Lưu trữ toàn bộ dữ liệu log thô từ Spark Streaming đổ xuống, bổ sung dấu thời gian hệ thống (`ingested_at`).
2.  **Silver (Lớp tích hợp):** Làm sạch dữ liệu, lọc bỏ các bản ghi lỗi kỹ thuật, chuẩn hóa định dạng vùng miền và thực hiện phép Join dữ liệu Log với bảng danh mục siêu dữ liệu của các trạm BTS dựa trên mã `cell_id`.
3.  **Gold (Lớp Nghiệp vụ / KPIs):** Áp dụng **Incremental Models (Mô hình tăng trưởng)** để tối ưu hiệu năng. dbt chỉ quét vùng dữ liệu delta mới phát sinh trong 15 phút gần nhất để tính toán các chỉ số KPI viễn thông cốt lõi:
    * *Tỷ lệ rớt cuộc gọi (Drop Call Rate):* $\text{Drop Call Rate} = \frac{\text{Tổng số cuộc gọi lỗi}}{\text{Tổng số cuộc gọi phát sinh}} \times 100$
    * *Lưu lượng Data:* Tổng dung lượng Download/Upload (GB/TB) phân tách theo dải băng tần và vùng địa lý.

### 3.4. Tầng Tự động hóa & Kiểm soát Chất lượng (Orchestration & Data Quality Layer)
* **Công nghệ:** Apache Airflow & dbt Tests.
* **Cơ chế hoạt động:**
    * **dbt Tests:** Định nghĩa trực tiếp các bộ quy tắc ràng buộc chất lượng dữ liệu (Data Quality Tests) tự động trong file cấu hình `schema.yml`. Đảm bảo 100% dữ liệu lớp Gold đạt chuẩn (ví dụ: Tọa độ GPS trạm BTS phải nằm trong phạm vi lãnh thổ Việt Nam, `Drop Call Rate` không vượt quá giá trị logic hợp lệ).
    * **Apache Airflow:** Hệ thống điều phối trung tâm (Orchestrator). Airflow quản lý vòng đời của pipeline theo chu kỳ phân tích, kích hoạt dbt thực thi mô hình tính toán tăng trưởng và chạy các bộ test chất lượng. Nếu `dbt test` thất bại, Airflow lập tức ngắt mạch pipeline, gửi cảnh báo hệ thống và ngăn chặn dữ liệu lỗi hiển thị lên báo cáo.

### 3.5. Tầng Trực quan hóa (Serving & Visualization Layer)
* **Công nghệ:** Apache Superset.
* **Cơ chế hoạt động:** Connect trực tiếp vào Trino Engine để khai thác các tệp dữ liệu tinh gọn từ lớp Gold. Superset xây dựng **Dashboard bản đồ nhiệt (Geographical Heatmap)** dựa trên hệ tọa độ kinh độ/vĩ độ của các trạm phát sóng BTS, giúp ban điều hành và đội ngũ kỹ thuật VTS dễ dàng phát hiện các "điểm đen" suy giảm tín hiệu hoặc trạm phát sóng đang quá tải theo thời gian thực.

---

## 4. MA TRẬN LUỒNG DỮ LIỆU ĐẦU CUỐI (END-TO-END DATA FLOW)

| Tầng Dữ Liệu | Công Nghệ Áp Dụng | Logic / Cơ Chế Kỹ Thuật | Đầu Ra Deliverables |
| :--- | :--- | :--- | :--- |
| **Ingestion** | Kafka, Spark Streaming | Micro-batching, Parse thô, Append-only | Khai thông dòng chảy dữ liệu liên tục vào Storage. |
| **Staging (Bronze)**| MinIO, Apache Iceberg | Snapshot Isolation, Schema Evolution | Bảng dữ liệu thô bền vững, phân vùng sơ bộ. |
| **Silver** | Trino, dbt | Loại bỏ trùng lặp, chuẩn hóa kiểu dữ liệu, Join Metadata | Bảng dữ liệu sạch, sẵn sàng cho việc tính toán KPI. |
| **Gold** | Trino, dbt (Incremental) | Chỉ xử lý dữ liệu Delta tăng trưởng, Aggregate KPIs | Tập dữ liệu Aggregated tinh gọn, truy vấn tốc độ cao. |
| **Automation** | Apache Airflow, dbt Tests | Điều phối DAGs, Gác cổng dữ liệu, Bắn Cảnh báo | Hệ thống vận hành tự động, Lineage tự động trực quan. |
| **Serving** | Apache Superset | OLAP Direct Query thông qua Trino Connector | Real-time Heatmap Dashboard giám sát trải nghiệm. |

---

## 5. ĐIỂM NHẤN CÔNG NGHỆ & LỢI THẾ CẠNH TRANH CỦA ĐỀ TÀI

1.  **Kiến trúc Đón đầu Xu hướng (MDS Lakehouse):** Sự kết hợp giữa Iceberg + Trino + dbt loại bỏ hoàn toàn sự rườm rà, chậm chạp của kiến trúc Hadoop/Hive truyền thống. Tiết kiệm tối đa tài nguyên I/O nhờ cơ chế xử lý in-place và quản lý siêu dữ liệu tối tân.
2.  **Tối ưu hóa Hiệu năng Thực tế (Incremental Compute):** Áp dụng nhuần nhuyễn tư duy tối ưu hóa Database vào cấu trúc dữ liệu lớn thông qua `Incremental Models` và cơ chế `Hidden Partitioning` của Iceberg, giải quyết triệt bài toán Big Data quy mô doanh nghiệp viễn thông.
3.  **Quản trị Dữ liệu Chuẩn Enterprise:** 100% logic nghiệp vụ được mã hóa tường minh (`Code-based`), dễ dàng quản lý phiên bản qua Git (CI/CD), tự động cập nhật sơ đồ `Data Lineage` mạch lạc từ dbt docs giúp hệ thống có tính minh bạch tuyệt đối trước Hội đồng chấm điểm.
