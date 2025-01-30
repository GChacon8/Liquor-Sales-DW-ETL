# Data Warehouse para Ventas y Producción de Licores  

Este proyecto implementa un Data Warehouse para analizar datos de ventas y producción de licores, utilizando un proceso ETL desarrollado en **Python** y una base de datos **SQLite**.  

## 📌 **Requisitos del sistema**  

Antes de ejecutar el proyecto, asegúrate de tener instalado lo siguiente:  

### **1. Instalación de Python**  
El código está desarrollado en **Python 3.x**, por lo que se recomienda instalarlo desde:  
- [Python.org](https://www.python.org/downloads/)  
- Usar `python --version` para verificar la instalación.  

### **2. Instalación de las dependencias**  
Se requiere instalar las siguientes bibliotecas:  

```sh
pip install pandas sqlite3 matplotlib

### **3. Estructura del proyecto**  


📂 data_warehouse
│── sales.csv               # Archivo de datos crudos
│── data_warehouse.db        # Base de datos SQLite generada
│── ETLP2.py           # Script principal para ETL               
│── README.md                # Documentación del proyecto

### **4. Ejecución**
python ETLP2.py
  
