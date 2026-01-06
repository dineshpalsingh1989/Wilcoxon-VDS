# Wilcoxon Vibration Data System (VDS) Cloud Dashboard

[![Live Demo](https://img.shields.io/badge/demo-online-green.svg?style=for-the-badge&logo=appveyor)](http://213.111.157.130:8050)
![Python](https://img.shields.io/badge/Python-3.9%2B-blue?style=for-the-badge&logo=python&logoColor=white)
![Dash](https://img.shields.io/badge/Dash-v2.0-0081CB?style=for-the-badge&logo=plotly&logoColor=white)
![Platform](https://img.shields.io/badge/Platform-Raspberry%20Pi-red?style=for-the-badge&logo=raspberrypi&logoColor=white)

---

### 📡 Industrial IoT Dashboard for Vibration Monitoring

**Wilcoxon-VDS** is a comprehensive Industrial IoT (IIoT) system designed to monitor vibration sensors in real-time. It acts as a central hub—ingesting data via MQTT, storing it locally, and visualizing it on a responsive web dashboard. It also bridges the gap to legacy automation systems by serving data out via **Modbus TCP** and **OPC UA**.

## 🚀 Key Features

| Feature | Description |
| :--- | :--- |
| **📊 Real-Time Dashboard** | Live monitoring of Velocity, Acceleration, Displacement, and RPM using **Plotly Dash**. |
| **📈 Advanced Analysis** | On-demand rendering of **Time Waveforms** and **FFT Spectrums** (Frequency Domain). |
| **🗄️ Data Historian** | Built-in **SQLite** storage with a "History Explorer" to search and export data to **Excel/CSV**. |
| **🔌 Protocol Gateway** | • **MQTT Client**: Ingests high-speed sensor data.<br>• **Modbus TCP Server**: Exposes metrics to PLCs on Port 502.<br>• **OPC UA Server**: Industry 4.0 standard integration on Port 4840. |
| **🖥️ System Health** | Monitors the Raspberry Pi CPU load, temperature, and memory usage. |

## 🛠️ Tech Stack

* ![Python](https://img.shields.io/badge/Language-Python-blue) **Core Logic**
* ![Plotly](https://img.shields.io/badge/Frontend-Dash%20%2F%20Plotly-orange) **Visualization**
* ![SQLite](https://img.shields.io/badge/Database-SQLite-lightgrey) **Storage**
* ![MQTT](https://img.shields.io/badge/Protocol-MQTT-purple) **Ingestion**
* ![OPC UA](https://img.shields.io/badge/Protocol-OPC%20UA-blue) **Integration**

## ⚙️ Installation

1.  **Clone the repository**:
    ```bash
    git clone [https://github.com/dineshpalsingh1989/Wilcoxon-VDS.git](https://github.com/dineshpalsingh1989/Wilcoxon-VDS.git)
    cd Wilcoxon-VDS
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

3.  **Run the application**:
    ```bash
    python app.py
    ```

## 🖥️ Accessing the Dashboard

Once running, open your browser and navigate to:

> **http://localhost:8050**

*(If running on a Raspberry Pi, use `http://<Pi-IP-Address>:8050`)*

## ⚠️ Critical Note

This project strictly requires **`pymodbus==2.5.3`**.
Do not upgrade to PyModbus v3.x+, as the API has changed significantly and will break the Modbus Server functionality.

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
