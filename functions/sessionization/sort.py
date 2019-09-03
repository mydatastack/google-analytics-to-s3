def main(xs: list, param: str) -> list:
    return sorted(xs, key=lambda x: int(x.replace('body_pr', '').replace(param, '')))




if __name__ == '__main__':
    import unittest

    ca = ['body_pr10ca', 'body_pr11ca', 'body_pr12ca', 'body_pr13ca', 'body_pr14ca', 'body_pr15ca', 'body_pr16ca', 'body_pr17ca', 'body_pr18ca', 'body_pr19ca', 'body_pr1ca', 'body_pr20ca', 'body_pr2ca', 'body_pr3ca', 'body_pr4ca', 'body_pr5ca', 'body_pr6ca', 'body_pr7ca', 'body_pr8ca', 'body_pr9ca']


    res_check = ['body_pr1ca', 'body_pr2ca', 'body_pr3ca', 'body_pr4ca', 'body_pr5ca', 'body_pr6ca', 'body_pr7ca', 'body_pr8ca', 'body_pr9ca', 'body_pr10ca', 'body_pr11ca', 'body_pr12ca', 'body_pr13ca', 'body_pr14ca', 'body_pr15ca', 'body_pr16ca', 'body_pr17ca', 'body_pr18ca', 'body_pr19ca', 'body_pr20ca']

    class TestHandler(unittest.TestCase):

        def test_page_path_level_one(self):
            self.assertEqual(main(ca, 'ca'), res_check)

    unittest.main()
