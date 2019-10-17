from main import parse_page_path

def test_construct_levels(url):
    return parse_page_path(url)

assert test_construct_levels("http://www.example.com/hello-world/") == ["/hello-world", "", "", ""]



