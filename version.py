def get_release(self):
    import requests

    res = requests.head("http://su13/outbreak/growth_rate/grs.csv.gz")
    return res.headers["Last-Modified"]
