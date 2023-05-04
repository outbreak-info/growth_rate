def get_release(self):
    from xml.etree import ElementTree as ET

    import requests

    res = requests.get("http://storage.googleapis.com/andersen-lab_temp").content
    root = ET.fromstring(res)
    # we cannot use text() to search b/c xml does not support text()
    # https://stackoverflow.com/questions/33830821/python-xpath-syntaxerror-invalid-predicate
    # .//{http://doc.s3.amazonaws.com/2006-03-01}Key[text()='outbreak_info/significant.csv']/following-sibling::{http://doc.s3.amazonaws.com/2006-03-01}LastModified
    last_modified = root.find(
        ".//{http://doc.s3.amazonaws.com/2006-03-01}Contents[{http://doc.s3.amazonaws.com/2006-03-01}Key='outbreak_info/new_gr_data.csv.gz']/{http://doc.s3.amazonaws.com/2006-03-01}LastModified"
    )
    return last_modified.text
