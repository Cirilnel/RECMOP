import asyncio
from playwright.async_api import async_playwright
import csv

URL = "https://webapps.enea.it/rfvp.nsf/rfvp.xsp"

async def estrai_dati_da_pagina(page):
    dati = []

    await page.wait_for_selector('table#view\\:_id1\\:viewPanel1')

    righe = await page.query_selector_all('table#view\\:_id1\\:viewPanel1 tbody tr')

    for riga in righe:
        celle = await riga.query_selector_all('td')
        if len(celle) < 6:
            continue
        marca = (await celle[0].inner_text()).strip()
        modello = (await celle[1].inner_text()).strip()
        potenza = (await celle[2].inner_text()).strip().replace(" Wp", "").replace(",", ".")
        efficienza = (await celle[3].inner_text()).strip().replace("%", "").replace(",", ".")
        dimensioni = (await celle[4].inner_text()).strip()
        tecnologia = (await celle[5].inner_text()).strip()

        dati.append({
            "Marca": marca,
            "Modello": modello,
            "Potenza (Wp)": potenza,
            "Efficienza (%)": efficienza,
            "Dimensioni (mm)": dimensioni,
            "Tecnologia": tecnologia
        })

    return dati

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await page.goto(URL)

        tutti_dati = []

        for pagina_num in range(4):
            print(f"Estraggo dati pagina {pagina_num + 1}")
            dati = await estrai_dati_da_pagina(page)
            tutti_dati.extend(dati)

            if pagina_num < 3:
                next_button = await page.query_selector('#view\\:_id1\\:viewPanel1\\:pager2__Next__lnk')
                if next_button:
                    await next_button.click()
                    await page.wait_for_timeout(3000)
                else:
                    print("Bottone next non trovato, termino qui.")
                    break

        with open("../offerta/panel/pannelli_enea_rfvp.csv", "w", newline="", encoding="utf-8") as f:
            campi = ["Marca", "Modello", "Potenza (Wp)", "Efficienza (%)", "Dimensioni (mm)", "Tecnologia"]
            writer = csv.DictWriter(f, fieldnames=campi, delimiter=';', lineterminator='\n')
            writer.writeheader()
            for riga in tutti_dati:
                writer.writerow(riga)

        print(f"Totale moduli estratti: {len(tutti_dati)}")

        await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
