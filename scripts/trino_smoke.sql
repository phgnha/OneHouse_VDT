-- Hiển thị tất cả các catalogs (nguồn dữ liệu) đã kết nối trong Trino
show catalogs;

-- Hiển thị tất cả các schemas (cơ sở dữ liệu con) bên trong catalog "viettel"
show schemas from viettel;

-- Liệt kê tất cả các bảng dữ liệu (tables) nằm trong schema "gold" của catalog "viettel"
show tables from viettel.gold;

-- Truy vấn lấy top 20 trạm BTS có mức tải cao nhất (cell_load_score) từ bảng gold_cell_heatmap
select
    cell_id,               -- Mã định danh của trạm thu phát sóng (BTS)
    province,              -- Tỉnh/Thành phố nơi đặt trạm
    district,              -- Quận/Huyện
    severity,              -- Mức độ nghiêm trọng của trạng thái tải (VD: NORMAL, HIGH, CRITICAL)
    drop_call_rate_pct,    -- Tỷ lệ phần trăm cuộc gọi bị rớt (Call Drop Rate)
    qoe_issue_rate_pct,    -- Tỷ lệ phần trăm trải nghiệm người dùng kém (Quality of Experience Issue Rate)
    cell_load_score,       -- Điểm tải trọng tổng hợp của trạm (Càng cao càng quá tải)
    data_traffic_gb        -- Tổng lưu lượng dữ liệu (Data) đã sử dụng qua trạm này (đơn vị: GB)
from viettel.gold.gold_cell_heatmap
order by cell_load_score desc -- Sắp xếp giảm dần theo mức độ tải
limit 20;                     -- Chỉ lấy 20 kết quả đầu tiên
