"""Isolated DOM fixtures for the same rich-text/quoted-reply boundary as live capture."""

from pathlib import Path

import pytest
from playwright.sync_api import sync_playwright

from packages.whatsapp.web_capture import _candidate_from_row, _extract_visible_rows


@pytest.mark.parametrize('quote_attribute', ['data-testid="quoted-message"', 'aria-label="Quoted message"'])
def test_rich_reply_retains_own_paragraphs_without_republishing_parent(quote_attribute):
    with sync_playwright() as playwright:
        if not Path(playwright.chromium.executable_path).exists():
            pytest.skip('This isolated DOM fixture requires installed Chromium')
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content(f'''<main id="main">
              <div data-pre-plain-text="[10:06 AM, 11/4/2026] Current Tenant: ">
                <div {quote_attribute}>
                  <span>Parent Tenant</span><span>+1 (212) 555-0106</span>
                  <span class="selectable-text">The elevator cannot be called from the basement.</span>
                </div>
                <span class="selectable-text">I rode it to the basement today.</span>
                <span class="selectable-text">Did a mechanic visit?</span>
              </div>
            </main>''')
            rows = _extract_visible_rows(page)
            assert len(rows) == 1
            candidate = _candidate_from_row('Fixture Tenants', rows[0])
            assert candidate.text == 'I rode it to the basement today.\nDid a mechanic visit?'
            assert 'cannot be called' in rows[0]['reply_text']
            assert 'cannot be called' not in candidate.text
            assert candidate.sender == 'Current Tenant'
        finally:
            browser.close()


def test_media_only_reply_does_not_use_quoted_text_as_fallback():
    with sync_playwright() as playwright:
        if not Path(playwright.chromium.executable_path).exists():
            pytest.skip('This isolated DOM fixture requires installed Chromium')
        browser = playwright.chromium.launch(headless=True)
        try:
            page = browser.new_page()
            page.set_content('''<main id="main">
              <div data-pre-plain-text="[10:06 AM, 11/4/2026] Current Tenant: ">
                <div data-testid="quoted-message"><span class="selectable-text">Both elevators are out.</span></div>
                <video width="200" height="200"></video>
              </div></main>''')
            rows = _extract_visible_rows(page)
            candidate = _candidate_from_row('Fixture Tenants', rows[0])
            assert candidate.text == 'video omitted'
            assert rows[0]['reply_text'] == 'Both elevators are out.'
        finally:
            browser.close()
