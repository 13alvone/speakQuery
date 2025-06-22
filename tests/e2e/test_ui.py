from playwright.sync_api import sync_playwright


def test_index_page(server):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.add_init_script(
            "window.axios={post:(url,data)=>fetch(url,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(data)}).then(r=>r.json()),get:(url)=>fetch(url).then(r=>r.json())};"
        )
        page.goto(f"{server}/")
        color = page.evaluate("getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim()")
        assert color == '#28a745'
        page.fill('#query', 'index="dummy"')
        with page.expect_response(lambda r: r.url.endswith('/run_query') and r.status == 200):
            page.click('#run-query-btn')
        browser.close()


def test_dropdown_interaction(server):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.add_init_script(
            "window.axios={post:(u,d)=>fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)}).then(r=>r.json()),get:(u)=>fetch(u).then(r=>r.json())};"
        )
        with page.expect_response(lambda r: r.url.endswith('/get_saved_searches') and r.status == 200):
            page.goto(f"{server}/saved_searches.html")
        color = page.evaluate("getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim()")
        assert color == '#28a745'
        browser.close()


def test_lookups_page(server):
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.add_init_script(
            "window.axios={post:(u,d)=>fetch(u,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(d)}).then(r=>r.json()),get:(u)=>fetch(u).then(r=>r.json())};"
        )
        page.goto(f"{server}/lookups.html")
        color = page.evaluate("getComputedStyle(document.documentElement).getPropertyValue('--primary-color').trim()")
        assert color == '#28a745'
        page.wait_for_selector('#uploadBtn')
        assert page.is_visible('#uploadBtn')
        browser.close()
