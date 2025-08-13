# CMS-Beneficiary2024-pipeline
> Proyek ini membangun end-to-end data pipeline untuk mengolah dataset Medicare [Beneficiary 2024](https://github.com/sdg-1/healthcare-claims-analytics-project) dari CMS (Centers for Medicare & Medicaid Services).Pipeline ini memproses data mulai dari Extract → Load → Transform (ELT) dari file CSV ke Google BigQuery, dengan DBT untuk transformasi & pemodelan data,serta Apache Airflow untuk orkestrasi. Hasil akhirnya adalah Data Mart yang siap digunakan untuk analisis, termasuk metrik bulanan, demografi peserta, status kepesertaan, dan indikator cakupan.

## End-to-End Data Architecture
![arsitektur](images/arsitektur.jpg)

Arsitektur sistem menggunakan:
- *Data Source*: Dataset Beneficiary dari [CMS.gov](https://data.cms.gov/collection/synthetic-medicare-enrollment-fee-for-service-claims-and-prescription-drug-event).
- *Data Integration*: Script Python untuk memuat data ke Google BigQuery pada layer `raw`.
- *Data Transformation*: DBT digunakan untuk membuat tabel `staging`, `model`, dan `data mart`.
- *Data Orchestration*: Apache Airflow mengatur urutan eksekusi job.
- *Visualization*: Looker Studio digunakan untuk membuat dashboard interaktif.
Flow:
`cms.gov → RAW → STAGING → MODEL → MART → Looker Studio`

## Data Model (Star Chema) Beneficiary 2024
![relasi](images/relasi.jpg)

Struktur Star Schema pada Data Warehouse:
- *Fact Table*: `fact_beneficiary_month` menyimpan metrik bulanan seperti status Medicare, buy-in, dual status, dan coverage months.
- *Dimension Tables*:
    - `dim_location`: Informasi lokasi (county, state, ZIP).
    - `dim_plan`: Informasi plan/kontrak beneficiary.
    - `dim_profile`: Profil demografis beneficiary.
    - `dim_enrollment`: Informasi pendaftaran dan sumber enrollment.
    - `dim_entitlemen`t: Alasan entitlement, ESRD indicator, termination code.

## Dags Design
> menggunakan airflow untuk orchestration pada proyek ini. disini ada 2 dags yang berfungsi antara lain:
### Data Integration --Python
![extrcat](images/ektrac.png)
- Membuat dataset baru di Google BigQuery menggunakan credentials.json melalui script Python.
- Mengunggah data mentah (raw data) dari CMS.gov ke tabel BigQuery.
- Menambahkan jeda proses untuk memastikan data berhasil terunggah sepenuhnya.
- Setelah proses extract selesai, Airflow secara otomatis menjalankan DAG Data Transformation untuk memproses data lebih lanjut.

### Data Transformation --DBT
![transform](images/trans.png)
- Membuat tabel staging di BigQuery dengan data yang sudah dibersihkan dan distandarisasi.
-  Membangun data model sesuai kebutuhan analisis, seperti tabel dimensi dan fakta.
- Melakukan data testing menggunakan DBT untuk memastikan kualitas data sesuai kriteria.
- Data Mart Membuat data mart siap pakai untuk analisis dan visualisasi.


## Dashboard
![profile](images/profile.png)

Visualisasi dashboard menunjukkan distribusi data beneficiary:
- *Bar Chart*: Distribusi jumlah beneficiary berdasarkan kelompok umur dan gender.
    - `Toddler` = untuk umur pasien  0 < 5 tahun
    - `child` = untuk umur pasien antara 5 - 9 tahun
    - `teenage` = untuk umur pasien antara 10 - 18 tahun
    - `adult` = untuk umur pasien antara 19 - 60 tahun
    - `elderly` = untuk pasien 60 tahun ke atas
- *Donut Chart*: Perbandingan gender pada tahun 2024.
- *KPI Card*: Menampilkan total beneficiary pada tahun 2024.

