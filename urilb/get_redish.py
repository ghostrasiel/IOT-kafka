"""取得redish連線."""

import rejson


class RedishOpPut():
    def __init__(self):
        self.client = rejson.Client(host='localhost',
                                    port=6379,
                                    password=123456
                                    )

    def send_redish_data(self, data: dict, key: str, find: str):
        """發送redish資料."""
        try:
            self.client.jsonset(key, '.'+find, data)
            return True
        except Exception as ex:
            print(ex)
            if self.get_redish_data(key) == None:
                self.client.jsonset(key, '.', {})
                self.client.jsonset(key, '.'+find, data)
                return True
            else:
                return False

    def get_redish_data(self, key: str, Path: str = '$'):
        """取得redish資料."""
        data = self.client.jsonget(key, rejson.Path(Path))
        return data


if '__main__' == __name__:
    redish = RedishOpPut()
    x = redish.send_redish_data(data={'machine': 'CP-A', 'timestamp': 1683857761,
                                      'temperature': 86, 'humidity': 91}, key="test_test", find='CP-B')
    print(x)
    # print(redish.get_redish_data('test_IOT', '$'))
