import os

from playwright.sync_api import sync_playwright

DATA_DIR = os.path.join(os.path.dirname(os.path.dirname(__file__)), "data")


def save_cookies_as_txt(cookies, file_path):
    with open(file_path, "w") as f:
        f.write("# Netscape HTTP Cookie File\n")
        f.write("# This file was generated by Playwright\n")
        for cookie in cookies:
            # Convert expiration date to integer if it exists
            expires = str(int(cookie["expires"])) if "expires" in cookie else "0"
            f.write(
                f"{cookie['domain']}\t"
                f"{'TRUE' if cookie['domain'].startswith('.') else 'FALSE'}\t"
                f"{cookie['path']}\t"
                f"{'TRUE' if cookie.get('secure', False) else 'FALSE'}\t"
                f"{expires}\t"
                f"{cookie['name']}\t"
                f"{cookie['value']}\n"
            )


def main():
    state_path = os.path.join(DATA_DIR, "state.json")
    options = {
        "headless": False,
        "args": [
            "--disable-blink-features=AutomationControlled",
            "--disable-extension",
            "--ignore-certificate-errors",
        ],
        "ignore_default_args": ["--enable-automation"],
        "slow_mo": 50,
        "channel": "chrome",
    }
    with sync_playwright() as p:
        browser = p.chromium.launch(**options)
        if os.path.exists(state_path):
            context = browser.new_context(storage_state=state_path)
        else:
            context = browser.new_context()
        page = context.new_page()

        # Navigate to YouTube and log in
        page.goto("https://www.youtube.com")
        # Allow manual login
        input("Press Enter after logging in... Do not close the window manually")

        # Get cookies after logging in
        cookies = context.cookies()

        # Save cookies to a file in cookies.txt format
        save_cookies_as_txt(cookies, os.path.join(DATA_DIR, "cookies.txt"))
        context.storage_state(path=state_path)

        browser.close()


if __name__ == "__main__":
    main()
