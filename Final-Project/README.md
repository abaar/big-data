# Big-Data Final Project

## Jumpto
1. [API](#api)
2. [Jumlah Model](#jumlah-model)
3. [Batasan](#batasan)
4. [Penjelasan Aplikasi](#penjelasan-aplikasi)
5. [How to Run](#how-to-run)
6. [Screenshots](#screenshots)

## API
1. `http://127.0.0.1:51629/show-models` : Melihat total model yang terbuat, dan masing-masing panjang data tiap model
2. `http://127.0.0.1:51629/model-cluster/{id}?latitude={x}&longitude={y}&sentiment={z}` : Model ke-`id` , dengan data `x`,`y`,`z,` sebagai bahan untuk memprediksi cluster.
3. `http://127.0.0.1:51629/show-cluster/{id}` : Menampilkan semua hasil prediksi untuk data ke-id dengan model ke-id

## Jumlah Model
Tidak ada batas jumlah `model`, karena aplikasi ini berjalan secara `STREAMING` sehingga ketika ada data baru, akan ditambahkan kedalam total data sebelumnya.
Dalam kasus ini , 1 Model dibentuk dalam kurun waktu `60 Detik`. 

Kira kira gambarannya seperti ini, T(i) adalah jumlah data yang didapatkan dalam batch pengambilan data ke-i, T(i+1) = T(i) + T(i+1)

## Batasan
Dataset yang dipakai adalah [Sentiment By Location](https://www.kaggle.com/jacksapper/company-sentiment-by-location) - 2577779 baris. Field yang digunakan untuk clustering adalah `Latitude`, `Longitude`, dan `Sentiment`.

## Penjelasan Aplikasi
### stream-test.py
Mesin utama streaming yang menerima data dari Kafka secara berkala, setiap data dari batch akan dijadikan model sesuai dengan yang telah dijelaskan pada bagian [Jumlah Model](#jumlah-model)

### Producer.py
Sebuah aplikasi sebagai `seeder` / seolah olah API agar seperti sedang melakukan streaming yang mengirimkan pesan dalam kurun waktu 1ms-5ms.

### App.py & Server.py
Server agar hasil pemodelan dan prediksi dapat diakses sebagai API

### Engine.py
Mesin pemodelan dan prediksi

## How to Run
1. Jalankan Zookeper
2. Jalankan Kafka
3. Buatlah Topic sesuai yang anda inginkan, disini saya menggunakan `bigdata`
4. Jalankan `Producer.py`
5. Jalankan 'stream-test.py'

## Screenshots
### Fitur 1

### Fitur 2

### Fitur 3
