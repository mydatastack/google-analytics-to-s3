def main(xs: list) -> list:
    return xs
    return [
            sublist
            for sublist in xs if any(v for v in sublist) 
            ] 





if __name__ == '__main__':
    import unittest

    lres = [
            ['Peitschen / Paddel, 42989','', 'Leder Peitsche schwarz geflochten 90 cm', 22.65, 1,], 
            ['Peitschen / Paddel, 42706','', 'Leder - Peitsche schwarz/rot', 33.57, 1,], 
            ['Cockringe, 50451','', 'Cockring Tri-Snap Scrotum Support Ring M black', 8.36, 1,], 
            ['Cockringe aus Silikon / Gummi', 48500,'', 'Cockring The Double Strangler', 5, 1,], 
            ['','','','','','',''], 
            ['','','','','','',''], 
            ['','','','','','',''], 
            ['','','','','','',''], 
            ['','','','','','',''], 
            ]

    res_check = [
            ['Peitschen / Paddel, 42989','', 'Leder Peitsche schwarz geflochten 90 cm', 22.65, 1,], 
            ['Peitschen / Paddel, 42706','', 'Leder - Peitsche schwarz/rot', 33.57, 1,], 
            ['Cockringe, 50451','', 'Cockring Tri-Snap Scrotum Support Ring M black', 8.36, 1,], 
            ['Cockringe aus Silikon / Gummi', 48500,'', 'Cockring The Double Strangler', 5, 1,], 
            ] 

    class TestHandler(unittest.TestCase):

        def test_page_path_level_one(self):
            self.assertEqual(main(lres), res_check)

    unittest.main()
