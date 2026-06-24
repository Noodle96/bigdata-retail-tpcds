#!/bin/bash

set -e

echo "======================================"
echo "Creando entorno virtual..."
echo "======================================"

python3 -m venv venv_agente

echo "======================================"
echo "Activando entorno virtual..."
echo "======================================"

source venv_agente/bin/activate

echo "======================================"
echo "Actualizando pip..."
echo "======================================"

python -m pip install --upgrade pip
pip install --upgrade pip

unalias python
unalias pip 2>/dev/null

echo "======================================"
echo "Instalando dependencias..."
echo "======================================"

pip install -r requirements.txt

echo "======================================"
echo "Instalación completada"
echo "Entorno activo: $(which python)"
echo "======================================"


