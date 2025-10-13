@echo off
rem #########################################################################
rem #
rem # Run g.mkfontcap outside a grass session during installation
rem #
rem #########################################################################
echo Setup of WinGRASS-8.4.1
echo Generating the font configuration file by scanning various directories for fonts.
echo Please wait. Console window will close automatically ....

rem set gisbase
set GISBASE=C:\Program Files\GRASS GIS 8.4

rem set path to freetype dll
set FREETYPEBASE=C:\Program Files\GRASS GIS 8.4\extrabin;C:\Program Files\GRASS GIS 8.4\lib

rem set dependencies path
set PATH=%FREETYPEBASE%;%PATH%

rem GISRC must be set
set GISRC=dummy

rem run g.mkfontcap outside a grass session
"%GISBASE%\bin\g.mkfontcap.exe" --overwrite
exit
