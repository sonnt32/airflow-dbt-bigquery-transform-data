# ============================================================
# Dockerfile — 1 image dùng chung cho tất cả game projects
# Build: docker build -f dbt_core/Dockerfile -t dbt-core:latest .
# ============================================================

FROM python:3.11-slim

# Cài dbt-bigquery (bao gồm dbt-core)
RUN pip install --no-cache-dir \
    dbt-bigquery==1.8.0

# Tạo thư mục làm việc
WORKDIR /dbt

# Copy toàn bộ cấu hình dbt
COPY dbt_core/dbt_project.yml  /dbt/dbt_project.yml
COPY dbt_core/profiles.yml     /dbt/profiles.yml

# Copy TẤT CẢ models của mọi game vào cùng 1 image
# dbt chỉ chạy models thuộc DBT_PROJECT_NAME được inject
COPY dbt_core/models/          /dbt/models/

# Copy macros dùng chung cho tất cả game projects
COPY dbt_core/macros/          /dbt/macros/

# Thư mục chứa service account key
# (được mount hoặc copy lúc runtime, không baked vào image)
RUN mkdir -p /dbt/keys

# Verify cài đặt thành công
CMD ["dbt", "--version"]