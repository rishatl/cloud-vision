# Латыпов Ришат, 11-904

# Задание 2. Обработка фотографий с лицами людей
### Бакеты
- itis-2022-2023-vvot22-photos
- itis-2022-2023-vvot22-faces
### БД
- vvot22-db-photo-face
### Очереди
- vvot22-tasks
### Триггеры
- vvot22-photo-trigger (сервисный аккаунт **vvot22-face-detection**)
- vvot22-task-trigger (сервисный аккаунт **vvot22-cut-invoker**)
### Функции
- vvot22-face-detection (сервисный аккаунт **vvot22-face-detection**, код из файла **function/crop.py**)
- vvot22-boot (сервисный аккаунт **vvot22-boot-function**, код из файла **function/bot.py**)
### Контейнер
- vvot22-face-cut (сервисный аккаунт **vvot22-cut-invoker**, код из папки **container**)
