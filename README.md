# 🎮 dbt_airflow_master — Airflow vận hành dbt trên Docker

> **Giai đoạn học tập:** Airflow vận hành 1 dự án dbt  
> **Tác giả:** Son Nguyen  
> **Dữ liệu:** Google Analytics 4 (Firebase) → BigQuery → dbt transform  
> **Môi trường:** Docker Desktop (Windows)

---

## 📌 Mục đích dự án

Đây là dự án **thực hành thứ 3** trong lộ trình học Data Engineering cá nhân:

| Giai đoạn | Nội dung | Trạng thái |
|---|---|---|
| 1 | dbt vận hành 1 dự án | ✅ Hoàn thành |
| 2 | dbt vận hành nhiều dự án | ✅ Hoàn thành |
| 3 | **Airflow vận hành 1 dự án dbt** | ✅ Dự án này |
| 4 | Airflow vận hành nhiều dự án dbt | 🔜 Kế hoạch tiếp theo |

Mục tiêu cụ thể của dự án này:
- Hiểu cách **Docker** đóng gói và cô lập môi trường dbt và Airflow
- Hiểu cách **Airflow Scheduler + LocalExecutor** điều phối pipeline tự động
- Hiểu cách **DockerOperator** kết nối Airflow với dbt container
- Xây dựng pipeline transform dữ liệu GA4 game mobile thực tế từ BigQuery

---

## 🏗️ Kiến trúc hệ thống

```
BigQuery (Nguồn dữ liệu thô)
    └── analytics_485408210.events_intraday_*
              │
              │  source() — khai báo trong source.yml
              ▼
┌─────────────────────────────────────────────┐
│         dbt_core  (Docker Image)            │
│                                             │
│  event_flatten_raw.sql                      │
│  ├── Làm phẳng event_params (UNNEST)        │
│  ├── Tạo surrogate_key (MD5)                │
│  └── Incremental + Partition by date        │
│              │                              │
│              │  ref() — tự động thứ tự      │
│              ▼                              │
│  event_base.sql                             │
│  ├── Bảng tổng hợp cốt lõi                 │
│  └── Incremental + Partition by date        │
│              │                              │
│              ▼                              │
│  dbt test — schema.yml                      │
│  └── unique + not_null trên surrogate_key   │
└─────────────────────────────────────────────┘
              ▲
              │  DockerOperator — gọi dbt container
              │
┌─────────────────────────────────────────────┐
│       airflow_platform  (Docker Compose)    │
│                                             │
│  Webserver  ──  Scheduler  ──  Postgres     │
│                     │                       │
│              DAG: annoying_puzzle_pipeline  │
│              Schedule: 0 1 * * * (8h VN)   │
│                                             │
│  Task 1: dbt run event_flatten_raw          │
│  Task 2: dbt run event_base          (>>)   │
│  Task 3: dbt test                    (>>)   │
└─────────────────────────────────────────────┘
```

---

## 📁 Cấu trúc thư mục

```
dbt_airflow_master/
│
├── .env                          # Biến môi trường (KHÔNG commit lên Git)
├── .gitignore                    # Bảo vệ credential và file tự sinh
│
├── dbt_core/                     # dbt Worker — đóng gói thành Docker Image
│   ├── Dockerfile                # Công thức build image dbt-annoying-puzzle:latest
│   ├── dbt_project.yml           # Khai báo project, materialization strategy
│   ├── profiles.yml              # Thông tin kết nối BigQuery
│   ├── .dbtignore                # File dbt bỏ qua khi build
│   └── models/
│       └── annoying-puzzle/
│           ├── source.yml        # Khai báo nguồn BigQuery (wildcard table)
│           ├── schema.yml        # Data quality tests
│           ├── staging/          # (Dự kiến) Tầng làm sạch dữ liệu thô
│           └── marts/
│               ├── event_flatten_raw.sql   # Làm phẳng GA4 event_params
│               └── event_base.sql          # Bảng base tổng hợp cốt lõi
│
├── airflow_platform/             # Airflow Master — Docker Compose
│   ├── docker-compose.yaml       # Khởi động Webserver + Scheduler + Postgres
│   ├── dags/
│   │   └── annoying_puzzle_dag.py  # Kịch bản điều phối pipeline
│   ├── logs/                     # Tự động sinh khi Airflow chạy
│   └── plugins/                  # Để trống, dùng mở rộng sau
│
└── shared_keys/                  # Credential BigQuery (KHÔNG commit lên Git)
    └── annoying-puzzle-dbt.json  # Google Service Account key
```

---

## 🚀 Hướng dẫn cài đặt và chạy

### Yêu cầu

- Docker Desktop (Windows/Mac/Linux)
- Git
- VS Code (khuyến nghị)
- Google Cloud account với BigQuery dataset GA4

### Bước 1 — Clone và chuẩn bị

```bash
git clone https://github.com/<your-username>/dbt_airflow_master.git
cd dbt_airflow_master
```

### Bước 2 — Cấu hình biến môi trường

Tạo file `.env` từ template:

```bash
cp .env.example .env
```

Điền thông tin vào `.env`:

```env
GCP_PROJECT_ID=your-gcp-project-id
GCP_DATASET=your_dataset_name
GCP_LOCATION=US
KEYFILE_PATH=/dbt/keys/your-service-account.json
AIRFLOW_UID=50000
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__LOAD_EXAMPLES=False
```

### Bước 3 — Đặt file Service Account key

```bash
# Đặt file JSON key vào thư mục shared_keys/
# File này đã được .gitignore bảo vệ
cp /path/to/your-service-account.json shared_keys/annoying-puzzle-dbt.json
```

### Bước 4 — Build Docker image cho dbt

```bash
# Chạy từ thư mục gốc dbt_airflow_master/
docker build \
  -f dbt_core/Dockerfile \
  -t dbt-annoying-puzzle:latest \
  .
```

Kiểm tra build thành công:

```bash
docker run --rm dbt-annoying-puzzle:latest
# Kết quả đúng: Core: installed: 1.8.0 | Plugins: bigquery: 1.8.0
```

### Bước 5 — Khởi động Airflow

```bash
cd airflow_platform

# Lần đầu: khởi tạo database và tạo user admin
docker compose run --rm airflow-init

# Khởi động toàn bộ Airflow
docker compose up -d
```

### Bước 6 — Truy cập Airflow UI

```
URL:      http://localhost:8080
Username: admin
Password: admin
```

Tìm DAG `annoying_puzzle_dbt_pipeline` → bật toggle → click **▶ Trigger DAG**

---

## ⚙️ Cấu hình chi tiết

### dbt_project.yml — Materialization Strategy

| Tầng | Kiểu | Lý do |
|---|---|---|
| `marts/event_flatten_raw` | `incremental` | Dữ liệu GA4 lớn — chỉ xử lý dữ liệu mới |
| `marts/event_base` | `incremental` | Phụ thuộc vào flatten — chạy sau |

**Incremental logic:**
- Lần đầu (`full-refresh`): quét 2 ngày gần nhất
- Các lần sau: chỉ quét 2 ngày gần nhất, bỏ qua surrogate_key đã tồn tại

### DAG Schedule

```python
schedule_interval='0 1 * * *'
# Chạy lúc 01:00 UTC = 08:00 sáng giờ Việt Nam (UTC+7)
# Mỗi ngày một lần, tự động
```

### Thứ tự Task trong DAG

```
dbt_run_event_flatten_raw
        >>
dbt_run_event_base
        >>
dbt_test
```

Thứ tự này được đảm bảo bởi `ref('event_flatten_raw')` trong SQL của `event_base` — dbt tự hiểu phụ thuộc và Airflow thực thi đúng thứ tự.

---

## 🔒 Bảo mật

Các file sau **tuyệt đối không được commit** lên Git:

```gitignore
.env                  # Chứa biến môi trường nhạy cảm
shared_keys/          # Chứa Google Service Account key
*.json                # Tránh lộ bất kỳ file JSON credential nào
```

Để chia sẻ dự án, dùng `.env.example` (không chứa giá trị thật):

```env
GCP_PROJECT_ID=
GCP_DATASET=
GCP_LOCATION=
KEYFILE_PATH=
AIRFLOW_UID=50000
```

---

## 🧠 Kiến thức kỹ thuật đã áp dụng

### Docker
- `Dockerfile` — đóng gói dbt và dependencies thành image tái sử dụng
- `docker-compose.yaml` — orchestrate nhiều service (Webserver, Scheduler, Postgres)
- Build context — hiểu cách Docker copy file từ host vào image

### Apache Airflow
- **DAG** — định nghĩa workflow dưới dạng code Python
- **DockerOperator** — gọi container độc lập từ bên trong DAG
- **LocalExecutor** — chạy task song song trong cùng máy với Scheduler
- **schedule_interval** — cron expression để lên lịch tự động

### dbt
- **Incremental model** — chỉ xử lý dữ liệu mới, tiết kiệm chi phí BigQuery
- **Surrogate key** — MD5 hash để nhận diện và dedup dữ liệu
- **source()** — khai báo bảng nguồn, tách biệt với model
- **ref()** — quản lý phụ thuộc giữa các model tự động
- **Partition + Cluster** — tối ưu chi phí query BigQuery
- **dbt test** — kiểm tra chất lượng dữ liệu tự động (unique, not_null)

### BigQuery / GA4
- Wildcard table `events_intraday_*` với `_TABLE_SUFFIX`
- UNNEST array — làm phẳng `event_params` lồng nhau
- `SELECT AS STRUCT` — nhóm nhiều phép tính trong một lần UNNEST

---

## 🔜 Kế hoạch phát triển tiếp theo

**Giai đoạn 4 — Airflow vận hành nhiều dự án dbt:**
- Thêm nhiều game project vào cùng một Airflow instance
- Mỗi game có DAG riêng, dbt project riêng, BigQuery dataset riêng
- Dùng Airflow Variables/Connections để quản lý cấu hình tập trung
- Xem xét chuyển sang `CeleryExecutor` khi số lượng task tăng lên

**Cải thiện hệ thống hiện tại:**
- Thêm tầng `staging/` để làm sạch dữ liệu thô trước khi vào marts
- Thêm Slack/Email alerting khi DAG thất bại
- Chuyển credential sang Google Secret Manager cho môi trường production
- Viết thêm `accepted_values` test cho `event_name` trong schema.yml

---

## 📚 Tài liệu tham khảo

- [dbt Documentation](https://docs.getdbt.com)
- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [GA4 BigQuery Export Schema](https://support.google.com/analytics/answer/7029846)
- [Docker Documentation](https://docs.docker.com)

---

*Dự án này được xây dựng như một phần trong lộ trình học Data Engineering cá nhân — từ dbt đơn lẻ đến hệ thống pipeline tự động hóa hoàn chỉnh với Airflow.*
