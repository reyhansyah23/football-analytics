import pandas as pd
import requests
from bs4 import BeautifulSoup
 
class WikiTableParser:
    def __init__(self, html_content):
        headers_http = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

        response = requests.get(html_content, headers=headers_http)
        self.soup = BeautifulSoup(response.text, 'html.parser')

        # """Initialize with HTML string or a soup object."""
        # if isinstance(html_content, BeautifulSoup):
        #     self.soup = html_content
        # else:
        #     self.soup = BeautifulSoup(html_content, 'html.parser')

        self.df = None

    def parse_managers_table(self, table_keyword='Managers'):
        """Finds the table by caption and extracts the data."""
        target_table = self._find_table(table_keyword)
        # for table in self.find_all('table'):
        #     caption = table.find('caption')
        #     if caption and 'Managers' in caption.get_text():
        #         target_table = table
        #         break
        
        if not target_table:
            print(f"Error: Could not find table with caption containing '{table_keyword}'")
            return None

        # 1. Extract Headers
        headers = [th.get_text(strip=True).split('[')[0] for th in target_table.find_all('th', scope='col')]

        # 2. Extract Rows
        rows_data = []
        # Target the tbody to avoid header-only rows
        tbody = target_table.find('tbody') or target_table
        
        for tr in tbody.find_all('tr'):
            cells = tr.find_all(['td', 'th'])
            if not cells:
                continue

            processed_row = []
            for cell in cells:
                # Look for flag icons (Nationality column)
                flag_span = cell.find('span', class_='flagicon')
                
                if flag_span:
                    # Priority: <a> title -> <img> alt -> text
                    link = flag_span.find('a')
                    img = flag_span.find('img')
                    
                    if link and link.has_attr('title'):
                        cell_val = link['title']
                    elif img and img.has_attr('alt'):
                        cell_val = img['alt']
                    else:
                        cell_val = cell.get_text(strip=True)
                else:
                    # Standard text clean up (removing footnotes like [1])
                    cell_val = cell.get_text(strip=True)
                
                processed_row.append(cell_val)

            # Ensure row length matches headers (handling Wikipedia colspans/rowspans)
            if len(processed_row) >= len(headers):
                rows_data.append(processed_row[:len(headers)])

        self.df = pd.DataFrame(rows_data, columns=headers)
        return self.df.loc[1:]

    def _find_table(self, keyword):
        """Internal helper to locate the table by caption."""
        for table in self.soup.find_all('table'):
            caption = table.find('caption')
            if caption and keyword.lower() in caption.get_text().lower():
                return table
        return None

# --- Usage Example ---
# parser = WikiTableParser(your_html_source)
# df = parser.parse_managers_table()
# if df is not None:
#     print(df.head())