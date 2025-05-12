import sys
import traceback
import requests

"""
  notifier = TelegramNotifier('6677036039:AAHbXH8dLbaHHlnA0M9fqN_QFa1TOpCwgwc', '5705352522')
  notifier.send_exception(sys.exc_info())
  notifier.send_message("✅ Задача завершена!")
"""

class TelegramNotifier:
    def __init__(self, token='6677036039:AAHbXH8dLbaHHlnA0M9fqN_QFa1TOpCwgwc', chat_id='5705352522'):
        """
        Инициализация TelegramNotifier.
        
        :param token: Токен вашего бота (строка). Если None, будет запрошено у пользователя.
        :param chat_id: ID чата или пользователя (строка или число). Если None, будет запрошено у пользователя.
        """
        self.token = token or self._get_input("Введите токен бота: ")
        self.chat_id = str(chat_id or self._get_input("Введите chat_id: "))
        self.base_url = f"https://api.telegram.org/bot{self.token}/"

    @staticmethod
    def _get_input(prompt):
        """
        Вспомогательный метод для получения ввода от пользователя.
        
        :param prompt: Текст запроса.
        :return: Введенные данные (строка).
        """
        return input(prompt).strip()

    def send_message(self, text, parse_mode='Markdown'):
        """
        Отправка текстового сообщения в Telegram.
        
        :param text: Текст сообщения (строка).
        :param parse_mode: Форматирование текста ('Markdown' или 'HTML').
        :return: Ответ от Telegram API (словарь) или None при ошибке.
        """
        url = self.base_url + "sendMessage"
        data = {
            'chat_id': self.chat_id,
            'text': text,
            'parse_mode': parse_mode
        }
        try:
            response = requests.post(url, data=data)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            print(f"Ошибка отправки сообщения: {e}")
            return None

    def send_exception(self, exc_info):
        """
        Отправка информации об исключении в Telegram.
        
        :param exc_info: Информация об исключении (sys.exc_info()).
        :return: Ответ от Telegram API (словарь) или None при ошибке.
        """
        exc_type, exc_value, exc_traceback = exc_info
        error_text = (
            f"❌ *Произошла ошибка:*\n"
            f"Тип: `{exc_type.__name__}`\n"
            f"Сообщение: `{str(exc_value)}`\n\n"
            f"Трассировка:\n```\n{traceback.format_exc()}\n```"
        )
        return self.send_message(error_text)

    @staticmethod
    def handle_exception(notifier=None):
        """
        Глобальный обработчик исключений.
        
        :param notifier: Экземпляр TelegramNotifier (опционально).
        """
        def exception_handler(exc_type, exc_value, exc_traceback):
            if notifier:
                notifier.send_exception((exc_type, exc_value, exc_traceback))
            sys.__excepthook__(exc_type, exc_value, exc_traceback)
        sys.excepthook = exception_handler

#from telegram_notifier import TelegramNotifier

# Создаем экземпляр TelegramNotifier без явного указания токена и chat_id
# Они будут запрошены у пользователя при инициализации
#notifier = TelegramNotifier()

# Включаем глобальный обработчик исключений
#notifier.handle_exception(notifier)

# Пример функции с ошибкой
#def risky_function():
#    raise ValueError("Пример ошибки")
#
#if __name__ == "__main__":
    # Отправляем тестовое сообщение
#    notifier.send_message("✅ Программа запущена!")

#   try:
        # Вызываем функцию, которая может вызвать ошибку
#        risky_function()
#    except Exception as e:
        # Логируем ошибку вручную (если нужно)
#        notifier.send_exception(sys.exc_info())

    # Этот код не выполнится из-за ошибки выше
#    notifier.send_message("🎉 Программа завершена успешно!")
