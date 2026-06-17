#!/bin/bash

set -e

QUERY_FILE="$1"
QUERY_NAME="$2"
OUT_DIR="$3"

if [ -z "$QUERY_FILE" ] || [ -z "$QUERY_NAME" ] || [ -z "$OUT_DIR" ]; then
  echo "Uso:"
  echo "./hive/scripts/run_hive_query_with_metrics.sh <query_file.hql> <query_name> <output_dir>"
  exit 1
fi

if [ ! -f "$QUERY_FILE" ]; then
  echo "ERROR: No existe el archivo de consulta: $QUERY_FILE"
  exit 1
fi

mkdir -p "$OUT_DIR"

MAIN_LOG="${OUT_DIR}/${QUERY_NAME}.log"
JOBS_FILE="${OUT_DIR}/${QUERY_NAME}_jobs.txt"
APPS_FILE="${OUT_DIR}/${QUERY_NAME}_applications.txt"
SUMMARY_FILE="${OUT_DIR}/${QUERY_NAME}_summary.txt"

echo "============================================================" | tee "$SUMMARY_FILE"
echo "Consulta: $QUERY_NAME" | tee -a "$SUMMARY_FILE"
echo "Archivo: $QUERY_FILE" | tee -a "$SUMMARY_FILE"
echo "Inicio: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$SUMMARY_FILE"
echo "============================================================" | tee -a "$SUMMARY_FILE"

echo ""
echo "Ejecutando consulta Hive..."
echo ""

(time hive -f "$QUERY_FILE") 2>&1 | tee "$MAIN_LOG"

echo ""
echo "Consulta finalizada."
echo ""

echo "Fin: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$SUMMARY_FILE"

grep -o "job_[0-9_]*" "$MAIN_LOG" | sort -u > "$JOBS_FILE" || true
grep -o "application_[0-9_]*" "$MAIN_LOG" | sort -u > "$APPS_FILE" || true

{
  echo ""
  echo "================ TIEMPO ================="
  grep "Time taken:" "$MAIN_LOG" | tail -n 1 || true
  grep "^real" "$MAIN_LOG" || true
  grep "^user" "$MAIN_LOG" || true
  grep "^sys" "$MAIN_LOG" || true

  echo ""
  echo "================ JOBS MAPREDUCE ================="
  if [ -s "$JOBS_FILE" ]; then
    cat "$JOBS_FILE"
  else
    echo "No se encontraron jobs MapReduce."
  fi

  echo ""
  echo "================ APPLICATIONS YARN ================="
  if [ -s "$APPS_FILE" ]; then
    cat "$APPS_FILE"
  else
    echo "No se encontraron applications YARN."
  fi

  echo ""
  echo "================ CPU MAPREDUCE ================="
  grep "Total MapReduce CPU Time Spent" "$MAIN_LOG" || true
  grep "Stage-Stage" "$MAIN_LOG" || true

  echo ""
  echo "================ HDFS READ / WRITE POR STAGE ================"
  grep "Stage-Stage" "$MAIN_LOG" || true

  echo ""
  echo "================ HDFS READ / WRITE TOTAL ================"
  awk '
    /Stage-Stage/ {
      for (i=1; i<=NF; i++) {
        if ($i=="Read:") read += $(i+1);
        if ($i=="Write:") write += $(i+1);
      }
    }
    END {
      printf "HDFS Read Total Bytes: %d\n", read;
      printf "HDFS Write Total Bytes: %d\n", write;
      printf "HDFS Read Total GB: %.4f\n", read/1000000000;
      printf "HDFS Write Total MB: %.4f\n", write/1000000;
    }
  ' "$MAIN_LOG"

} | tee -a "$SUMMARY_FILE"

echo ""
echo "Capturando historial MapReduce..."
echo ""

if [ -s "$JOBS_FILE" ]; then
  while read JOB_ID; do
    HISTORY_LOG="${OUT_DIR}/${QUERY_NAME}_${JOB_ID}_history.log"

    echo "Capturando historial de $JOB_ID"
    mapred job -history all "$JOB_ID" > "$HISTORY_LOG" 2>&1 || true

  done < "$JOBS_FILE"
fi

echo ""
echo "Capturando reportes YARN..."
echo ""

{
  echo ""
  echo "================ YARN RESOURCE ALLOCATION ================"
} | tee -a "$SUMMARY_FILE"

if [ -s "$APPS_FILE" ]; then
  while read APP_ID; do
    APP_LOG="${OUT_DIR}/${QUERY_NAME}_${APP_ID}_yarn.log"

    yarn application -status "$APP_ID" > "$APP_LOG" 2>&1 || true

    {
      echo ""
      echo "Application: $APP_ID"
      grep "Application-Type" "$APP_LOG" || true
      grep "State" "$APP_LOG" || true
      grep "Final-State" "$APP_LOG" || true
      grep "Aggregate Resource Allocation" "$APP_LOG" || true
    } | tee -a "$SUMMARY_FILE"

  done < "$APPS_FILE"
else
  echo "No se encontraron applications YARN para capturar memoria." | tee -a "$SUMMARY_FILE"
fi

{
  echo ""
  echo "================ RESUMEN NUMÉRICO FINAL ================"

  HIVE_TIME=$(grep "Time taken:" "$MAIN_LOG" | tail -n 1 | sed -E 's/.*Time taken: ([0-9.]+) seconds.*/\1/' || true)
  REAL_TIME=$(grep "^real" "$MAIN_LOG" | tail -n 1 | awk '{print $2}' || true)
  CPU_TEXT=$(grep "Total MapReduce CPU Time Spent" "$MAIN_LOG" | tail -n 1 || true)

  HDFS_READ_TOTAL=$(awk '
    /Stage-Stage/ {
      for (i=1; i<=NF; i++) {
        if ($i=="Read:") read += $(i+1);
      }
    }
    END { printf "%d", read }
  ' "$MAIN_LOG")

  HDFS_WRITE_TOTAL=$(awk '
    /Stage-Stage/ {
      for (i=1; i<=NF; i++) {
        if ($i=="Write:") write += $(i+1);
      }
    }
    END { printf "%d", write }
  ' "$MAIN_LOG")

  TOTAL_MEMORY_MB_SECONDS=0
  TOTAL_VCORE_SECONDS=0

  for APP_LOG in "${OUT_DIR}/${QUERY_NAME}"_application_*_yarn.log; do
    if [ -f "$APP_LOG" ]; then
      LINE=$(grep "Aggregate Resource Allocation" "$APP_LOG" || true)
      MEM=$(echo "$LINE" | sed -E 's/.*: ([0-9]+) MB-seconds, ([0-9]+) vcore-seconds.*/\1/' || echo 0)
      VCORE=$(echo "$LINE" | sed -E 's/.*: ([0-9]+) MB-seconds, ([0-9]+) vcore-seconds.*/\2/' || echo 0)

      if [[ "$MEM" =~ ^[0-9]+$ ]]; then
        TOTAL_MEMORY_MB_SECONDS=$((TOTAL_MEMORY_MB_SECONDS + MEM))
      fi

      if [[ "$VCORE" =~ ^[0-9]+$ ]]; then
        TOTAL_VCORE_SECONDS=$((TOTAL_VCORE_SECONDS + VCORE))
      fi
    fi
  done

  echo "Query Name: $QUERY_NAME"
  echo "Hive Time Seconds: $HIVE_TIME"
  echo "Real Time: $REAL_TIME"
  echo "CPU MapReduce Text: $CPU_TEXT"
  echo "YARN Memory MB-seconds Total: $TOTAL_MEMORY_MB_SECONDS"
  echo "YARN VCore-seconds Total: $TOTAL_VCORE_SECONDS"
  echo "HDFS Read Bytes Total: $HDFS_READ_TOTAL"
  echo "HDFS Write Bytes Total: $HDFS_WRITE_TOTAL"

} | tee -a "$SUMMARY_FILE"

echo ""
echo "Resumen guardado en:"
echo "$SUMMARY_FILE"