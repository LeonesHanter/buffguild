#!/bin/bash
echo "=== Логи buffguild ==="
echo "1. Следить за логами в реальном времени"
echo "2. Последние 100 строк"
echo "3. Логи за сегодня"
echo "4. Логи с ошибками"
echo "5. Полный журнал"
echo -n "Выберите вариант (1-5): "
read choice

case $choice in
    1) sudo journalctl -u buffguild -f ;;
    2) sudo journalctl -u buffguild -n 100 ;;
    3) sudo journalctl -u buffguild --since today ;;
    4) sudo journalctl -u buffguild -p err --since "1 hour ago" ;;
    5) sudo journalctl -u buffguild --no-pager ;;
    *) echo "Неверный выбор" ;;
esac
