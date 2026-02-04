#!/bin/bash
echo "=== Управление buffguild ==="
echo "1. Запустить"
echo "2. Остановить"
echo "3. Перезапустить"
echo "4. Статус"
echo "5. Включить автозагрузку"
echo "6. Выключить автозагрузку"
echo -n "Выберите действие (1-6): "
read action

case $action in
    1) sudo systemctl start buffguild ;;
    2) sudo systemctl stop buffguild ;;
    3) sudo systemctl restart buffguild ;;
    4) sudo systemctl status buffguild ;;
    5) sudo systemctl enable buffguild ;;
    6) sudo systemctl disable buffguild ;;
    *) echo "Неверный выбор" ;;
esac
