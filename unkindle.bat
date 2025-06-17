cd /d "%~dp0"

set /p email=Email: 
C:\Python311\python.exe unkindle.py --loginonly --email %email% fake-asin
echo Login Success!
set /p downloadn=how many book to download: 
echo push enter AFTER you purchase the books.
pause

echo Parallel Download bat
del download_new_book.bat
C:\Python311\python.exe unkindle.py --loginonly  --email %email% --downloadn %downloadn% fake-asin

call download_new_book.bat
