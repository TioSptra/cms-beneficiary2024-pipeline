# Capstone Project Module 3

Setelah menyelesaikan pembelajaran modul 3, siswa telah mempelajari terkait tools-tools & concept data engineering dan cloud services, seperti:

- Spark
- DBT
- Google Cloud Services
- Airflow
- Data Modelling
- Data Governance

## Capstone Overview

Dengan capstone project ini, siswa akan mencoba menggunakan materi yang telah dipelajari untuk menyelesaikan problem real-world Data Engineering End-to-End.

## Data Sources

Siswa diharapkan dapat melakukan set up data source nya sendiri (jangan gunakan NYC Taxi sebagai dataset).

Berikut beberapa rekomendasi:

- [MusicBrainz](https://musicbrainz.org/doc/MusicBrainz_Database)
- [Postgres Sample DBs](https://github.com/neondatabase-labs/postgres-sample-dbs)
- [Postgres air](https://github.com/hettie-d/postgres_air)
- [Spotify API](https://developer.spotify.com/documentation/web-api)
- [Bluesky API](https://docs.bsky.app/)
- [List of Public APIs](https://github.com/public-apis/public-apis)
- [Healthcare data](https://github.com/sdg-1/healthcare-claims-analytics-project)

**NOTE:** Sangat direkomendasikan menggunakan dataset healthcare data untuk menjadi portofolio yang bagus.

Set up data source harus automated/programmatically, tidak ada human intervention.

## Tasks & Deliverables

### Setup Environment

File `credentials.json` akan di berikan ke masing2 siswa sebagai kredensial akses ke bigquery.

NOTE PENTING!!!

JANGAN PUSH `credentials.json` KE GIT. BUAT `.gitignore` FILE UNTUK EXCLUDE `credentials.json` DARI PROSES PUSH.

JIKA TERDAPAT SISWA YANG MELAKUKAN PUSH `credentials.json` KE GIT, MAKA CREDENTIAL KEY AKAN DI REVOKE DAN CAPSTONE AKAN DI NILAI GAGAL.

### End to End Data Pipelines

- Buatlah DAG Airflow otomatis (schedule bebas) untuk melakukan proses ELT dari data source, ke destination BigQuery.
- Implementasikan data warehouse layering di BigQuery (raw, staging, model, mart).
- Implementasikan metode data modeling star schema untuk membuat fact tables dan dim tables.
- Siswa dapat memilih business case nya sendiri untuk data model yang dibuat.
- Gunakan Spark / DBT untuk proses ELT.
  - Jika menggunakan spark, gunakan kombinasi Google Cloud Storage sebagai data lake dan BigQuery sebagai data warehouse.
  - Jika menggunakan DBT, data integration dapat menggunakan script python, atau menggunakan tools data integration lain seperti Airbyte.
    - DBT make sure sudah bisa di setup akses dengan BigQuery.
- Implementasikan data quality check di layer model berdasarkan dimensi data quality.
- Implementasikan DAG alerting ke discord (buat 1 skenario DAG yang sengaja gagal, dan juga alerting untuk data quality check yang gagal).
- Implementasikan data mart berdasarkan data model yang di buat.
- Buat dashboard simple menggunakan looker studio, atau apache superset, dari data mart yang di buat.
- Semua proses didokumentasikan, dan juga dilakukan menggunakan GIT.

Bonus:

- Jika credentials, API key, webhook key, dll tidak ter-ekspos dalam kode, akan diberikan bonus poin.
- Jika menggunakan SCD type 2 untuk dimensi table yang memiliki attribute slowly changing, akan diberikan bonus poin.
  - **NOTE**: CASE INI HANYA UNTUK ATTRIBUTE YANG BENAR2 SLOWLY CHANGING, JANGAN ASAL BUAT.
  - **CONTOH SALAH**: attribut tanggal lahir adalah attribut FIX yang tidak akan pernah berubah.
  - **CONTOH BENAR**: attribut gaji adalah attribut SLOWLY CHANGING yang bisa berubah suatu saat.

## Grading

### Main Tasks

| **Criteria**          | **Point** | **Description**                                                                                                                                    |
| --------------------------- | --------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Video Explanation** | 15              | Siswa dapat memberikan penjelasan yang detail dan terstruktur terkait submission nya, seperti tujuan, alur program, kegunaan spesifik blok kode, dsb |
| **End to End Data Pipelines**           | 85              | Semua deliverables diserahkan & SESUAI dengan requirements. Dokumentasi lengkap. |
| **Bonus 1: Hidden credentials**           | 5              | Sensitive access credentials seperti password, API key, dll tidak ter ekspos di kode |
| **Bonus 2: SCD Type 2**           | 15              | Implementasi SCD Type 2 di slowly changing dimension table, dengan proses yang benar, idempotent, dan reversible |

## Deadline

Lama waktu pengerjaan Capstone Project Module 3 adalah sampai 15 Agustus 2025 23:59:00 WIB. Pengerjaan akan terhitung sejak H+1 setelah pengumuman Guideline Project.

## Submission Process

- Ikuti struktur folder pada contoh, lalu lakukan di folder dengan nama masing-masing
- Buat repo git menggunakan akun masing-masing (visibility repo boleh publik ataupun private, jika private, kirim invite ke <etchzel@gmail.com>)
- Submit code ke github di akun masing-masing
- Video penjelasan di submit ke Drive/Dropbox/Cloud storage masing-masing siswa
- Video penjelasan berdurasi maksimal 20 menit dan wajib mengaktifkan kamera depan atau webcam sehingga wajah siswa ada dalam rekaman video

## Notes

- Jika siswa mengumpulkan Capstone Project Module 2 melewati tenggat waktu yang sudah ditentukan, maka akan ada pengurangan poin untuk nilai akhir sebagai berikut:

  - Telat 1 detik sampai 24 jam: nilai akhir dikurangi 10 poin
  - Telat 24 jam sampai 72 jam: nilai akhir dikurangi 20 poin
  - Telat lebih dari 72 jam: nilai akhir menjadi 0

- Penggunaan LLM/AI dalam pengerjaan harus bijak, tidak boleh asal copy paste.

- Segala bentuk plagiarisme tidak akan ditoleransi dan mutlak diberikan nilai 0.
