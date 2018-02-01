#! /usr/bin/env python
# coding: utf-8

## kismau.py

## This file contains the main GUI and event handling logic for the Soci krysse-system.
## The main logic is contained in the class "Vindu" which is the main graphical entity the system is comprised off.
## All buying of goods is taken care of in the class "HandleKurv"

## Created by: Uknown Alien
## Date: Some time in the past

## Not created by (For the sake of having a name and a date): Tormod Haugland
## Date: 10/03/2015

import wx, os, codecs, time, datetime, pg, glob, random, logging, logging.config, re, sys

# For scheduling jobs
import threading

# Import our almighty DbHandler
from dbhandler import DbHandler as DbHandler

# konstanter for programmet - huske å endre dette...
# tegnkoder
BILDEVISERSEKUNDER = 10
ENTER = 13

# Brukt til å finne noen viktige bilder
BASE_DIR = "/home/ksg/soci_kismau/"


# hovedvinduet - parent til de andre og lytter for keystrokes
class Vindu(wx.Frame):
    def __init__(self, parent, id, title):
        wx.Frame.__init__(self, parent, wx.ID_ANY, title)
        self.SetCursor(wx.StockCursor(wx.CURSOR_BLANK))
        self.wnd = wx.Panel(self, -1)
        self.kortnummer = ""
        self.bildeviser = Bildeviser(self, -1)
        self.bildeviser.Show()
        self.handlekurv = None
        self.admin = None
        self.melding = None
        wx.EVT_CHAR(self.wnd, self.Trykk)
        self.wnd.SetFocus()
        self.Fit()
        self.Show(True)
        self.ShowFullScreen(True)

        # Add this window-instance as the main window-root of the program
        db.set_root(self)

    def Trykk(self, event):
        if isinstance(self.handlekurv, Handlekurv):
            self.handlekurv.Trykk(event)
        elif 'fffff' in self.kortnummer:  # hvis man taster spritspritspritspritsprit skal programmet avslutte og automatisk startes på nytt (feilrettingsgreie)
            self.Close(True)
        elif isinstance(self.admin, Bilde):
            self.admin.Destroy()
            self.admin = None
        elif event.GetKeyCode() == ENTER:
            kortnummer = None
            if len(self.kortnummer) == 10:
                kortnummer = self.kortnummer
            else:
                match = re.findall('([0-9]{6,10})_$', self.kortnummer)

                if len(match) == 0:  # ingen treff på regex - altså ikke trukket et samfundetkort
                    self.melding = Melding(self, u"Ikke kort: %s" % self.kortnummer, wx.RED, wx.BLACK)  # DEBUG
                    return
                else:
                    kortnummer = match[len(match) - 1]

            print(kortnummer)
            person = db.getPersonFromCard(kortnummer)
            if (not person):
                # Some cards may have a hidden 0
                if (kortnummer[0] == '0'):
                    second_attempt = kortnummer[1:]
                    person = db.getPersonFromCard(second_attempt)

            # This didn't work either
            if (not person):
                self.melding = Melding(self, "Kortnummeret %s er ukjent." % kortnummer, wx.RED, wx.BLACK)
            else:

                if (person["saldo"] < 0):
                    self.melding = Melding(self, "Du er svart!", wx.RED, wx.BLACK)
                elif (person["saldo"] < 10):
                    self.melding = Melding(self, "Du er ikke svart,\n men du har strengt talt ikke råd til noe...",
                                           wx.WHITE, wx.BLACK)
                else:
                    self.NyHandlekurv(person)

                # Legacy code. Keep for now?
                # if 'ccff' in self.kortnummer:
                # 	self.admin = SisteKryss(self)
                # 	self.admin.Show()
                # elif person[2] == False: #personen er svart...
                #	self.melding = Melding(self, "Du er svart!", wx.RED, wx.BLACK)
                # else:
                #	self.NyHandlekurv(person[0:2])

            self.kortnummer = ""  # null ut inntastingen
        else:
            self.kortnummer = self.kortnummer + chr(event.GetKeyCode())

    def NyHandlekurv(self, person):
        self.handlekurv = Handlekurv(self, -1, person)

    def FjernHandlekurv(self, tekst, bakgrunn, forgrunn):
        self.handlekurv.Destroy()
        self.handlekurv = None
        self.melding = Melding(self, tekst, bakgrunn, forgrunn)


class Melding(wx.Panel):
    def __init__(self, parent, tekst, bakgrunn=wx.BLACK, forgrunn=wx.WHITE):
        wx.Panel.__init__(self, parent, -1, wx.DefaultPosition, wx.GetDisplaySize())
        self.SetBackgroundColour(bakgrunn)
        tekst = wx.StaticText(self, -1, tekst, (0, wx.GetDisplaySize().GetHeight() / 2),
                              (wx.GetDisplaySize().GetWidth(), -1), style=wx.ALIGN_CENTRE)
        tekst.SetFont(wx.Font(26, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
        tekst.SetForegroundColour(forgrunn)
        self.timer = wx.PyTimer(self.Destroy)
        self.timer.Start(2000, wx.TIMER_ONE_SHOT)


class Bilde(wx.Panel):
    def __init__(self, parent):
        wx.Panel.__init__(self, parent, -1, wx.DefaultPosition, wx.GetDisplaySize())
        self.SetBackgroundColour(wx.BLACK)
        self.Hide()


class SisteKryss(Bilde):
    def __init__(self, parent):
        Bilde.__init__(self, parent)
        tekst = wx.StaticText(self, -1, u"Krysset de 24 siste timene - rangert etter siste kryss:\n\n", (0, 50),
                              (wx.GetDisplaySize().GetWidth(), -1), style=wx.ALIGN_CENTRE)
        tekst.SetForegroundColour(wx.WHITE)
        tekst.SetFont(wx.Font(24, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
        t = u""
        q = db.query(
            "SELECT navn, TO_CHAR(MAX(k.kryssetid),'DD.MM. HH24:MI') FROM \"SociPersoner\" p JOIN \"SociKryss\" k ON p.id=k.person GROUP BY p.id,p.navn ORDER BY MAX(k.kryssetid) DESC LIMIT 20;")

        for l in q.getresult():
            t = t + u"%s: %s\n" % (l[1].decode('utf-8'), l[0].decode('utf-8'))

        tekst = wx.StaticText(self, -1, t, (10, 100))
        tekst.SetForegroundColour(wx.WHITE)
        tekst.SetFont(wx.Font(12, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))


class Omsetning(Bilde):
    def __init__(self, parent):
        Bilde.__init__(self, parent)
        tekst = wx.StaticText(self, -1, u"Omsetning På Societeten\n\n", (0, 100), style=wx.ALIGN_CENTRE)
        tekst.SetForegroundColour(wx.WHITE)
        tekst.SetFont(wx.Font(40, wx.FONTFAMILY_SWISS, wx.ITALIC, wx.NORMAL))
        # tekst.Center()
        t = ""
        r = db.query(
            "SELECT SUM(pris) FROM \"Kryss\" WHERE TO_CHAR(kryssetid+interval '4 hours','YYYY MM DD') = TO_CHAR(NOW()+interval '4 hours','YYYY MM DD');")

        if (r == None):
            return

        r = r.getresult()[0][0]

        if r != None:
            t = t + u"I kveld: %s kr\n\n" % r
        r = db.query(
            "SELECT SUM(pris) FROM \"Kryss\" WHERE TO_CHAR(kryssetid,'YYYY IW') = TO_CHAR(NOW(),'YYYY IW');").getresult()[
            0][0]
        if r != None:
            t = t + u"Denne uka: %s kr\n\n" % r
        r = db.query(
            "SELECT SUM(pris) FROM \"Kryss\" WHERE TO_CHAR(kryssetid,'YYYY MM') = TO_CHAR(NOW(),'YYYY MM');").getresult()[
            0][0]
        if r != None:
            t = t + u"Denne måneden: %s kr\n\n" % r
        r = db.query(
            "SELECT SUM(pris) FROM \"Kryss\" WHERE EXTRACT(year FROM kryssetid) = EXTRACT(year FROM NOW());").getresult()[
            0][0]
        if r != None:
            t = t + u"I år: %s kr\n\n" % r

        tekst = wx.StaticText(self, -1, t, (100, 200))
        tekst.SetForegroundColour(wx.WHITE)
        tekst.SetFont(wx.Font(24, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))

    # tekst.Centre()


class Bursdager(Bilde):
    def __init__(self, parent):
        Bilde.__init__(self, parent)
        dberror = False
        try:
            r = db.query(
                "SELECT * FROM \"Personer\" WHERE EXTRACT(day FROM fodselsdato) = EXTRACT(day FROM NOW()) AND EXTRACT(month FROM fodselsdato) = EXTRACT(month FROM NOW()) ORDER BY navn;");
        except pg.InternalError:
            dberror = True

        if not dberror and r.ntuples() > 0:
            tekst = wx.StaticText(self, -1, u"Dagens Bursdagsbarn", (0, 100), (wx.GetDisplaySize().GetWidth(), -1),
                                  style=wx.ALIGN_CENTRE)
            tekst.SetForegroundColour(wx.WHITE)
            tekst.SetFont(wx.Font(40, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
            t = ""

            for k in r.dictresult():
                t = t + k['navn'].decode('utf-8') + "\n\n"

            t = t + "\n\nGratulerer!"
            tekst = wx.StaticText(self, -1, t, (0, 300), (wx.GetDisplaySize().GetWidth(), -1), style=wx.ALIGN_CENTRE)
            tekst.SetForegroundColour(wx.WHITE)
            tekst.SetFont(wx.Font(24, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
        else:
            t = u"Alt var bedre før! :)"
            tekst = wx.StaticText(self, -1, t, (0, 100), (wx.GetDisplaySize().GetWidth(), -1), style=wx.ALIGN_CENTRE)
            tekst.SetForegroundColour(wx.WHITE)
            tekst.SetFont(wx.Font(24, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))


class Galleri(Bilde):
    def __init__(self, parent):
        Bilde.__init__(self, parent)
        filer = glob.glob("galleri/*.jpg")

        if len(filer) > 0:
            self.bilde = wx.StaticBitmap(self, -1, wx.Image(random.choice(filer)).Scale(wx.GetDisplaySize().GetWidth(),
                                                                                        wx.GetDisplaySize().GetHeight()).ConvertToBitmap(),
                                         (0, 0), style=0)


class Bildeviser(wx.Panel):
    def __init__(self, parent, id):
        wx.Panel.__init__(self, parent, id, wx.DefaultPosition, wx.GetDisplaySize())
        self.SetBackgroundColour(wx.BLACK)
        # self.bilder = [Omsetning, Galleri, Bursdager]
        self.bilder = [Omsetning, Galleri, Bursdager]
        self.bildenummer = 0
        self.dettebilde = None
        self.nestebilde = None
        self.timer = wx.PyTimer(self.Oppdater)
        self.timer.Start(BILDEVISERSEKUNDER * 1000, wx.TIMER_CONTINUOUS)
        self.LastBilde()

    def Oppdater(self):
        self.VisBilde()
        self.LastBilde()

    def VisBilde(self):
        if isinstance(self.dettebilde, wx.Panel):
            self.dettebilde.Destroy()

        self.dettebilde = self.nestebilde
        self.dettebilde.Show()

    def LastBilde(self):
        self.nestebilde = self.bilder[self.bildenummer](self)

        if len(self.bilder) > self.bildenummer + 1:
            self.bildenummer = self.bildenummer + 1
        else:
            self.bildenummer = 0


class Handlekurv(wx.Panel):
    def __init__(self, parent, id, person):
        wx.Panel.__init__(self, parent, id, wx.DefaultPosition, wx.GetDisplaySize())
        self.person = person
        self.SetBackgroundColour(wx.BLACK)
        tekst = wx.StaticText(self, -1, "%s" % self.person['id'], (50, 20), style=wx.ALIGN_CENTRE)
        tekst.SetForegroundColour(wx.WHITE)
        tekst.SetFont(wx.Font(18, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
        tekst = wx.StaticText(self, -1, "%s" % self.person['navn'], (50, 40), style=wx.ALIGN_CENTRE)
        tekst.SetForegroundColour(wx.WHITE)
        tekst.SetFont(wx.Font(24, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
        self.kurv = []
        self.x = 0
        self.y = 100

        # Variable holding the sum of the HandleKurv
        self.sum = 0

    def Trykk(self, event):
        key = chr(event.GetKeyCode())

        if chr(event.GetKeyCode()) == 'r':
            self.LagreKjop()
        elif chr(event.GetKeyCode()) == 'w':
            self.Kanseller()
        else:
            self.LeggTilVare(key)

    def LeggTilVare(self, key):

        print key
        if not varer.Test(key):
            return False

        vare = varer.Get(key)

        print vare

        # If the person does not have enough money to buy the vare
        if (vare['pris'] + self.sum > self.person['saldo']):
            text = wx.StaticText(self, -1, "Du har ikke råd til denne varen, dessverre.", (50, 75),
                                 style=wx.ALIGN_CENTRE)
            text.SetForegroundColour(wx.RED)
            text.SetFont(wx.Font(20, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
            self.timer = wx.PyTimer(text.Destroy)
            self.timer.Start(2000, wx.TIMER_ONE_SHOT)
            return

        self.kurv.append((vare['id'], 1, vare['pris']))
        self.sum += vare['pris']

        bilde = vare['bilde']

        if self.x + bilde.GetWidth() >= wx.GetDisplaySize().GetWidth() - 100:
            self.x = 0
            self.y = self.y + bilde.GetHeight() + 10

        if self.y + bilde.GetHeight() >= wx.GetDisplaySize().GetHeight() - 10:
            tekst = wx.StaticText(self, -1, ". . .",
                                  (wx.GetDisplaySize().GetWidth() / 2 - 10, wx.GetDisplaySize().GetHeight() - 70),
                                  style=wx.ALIGN_CENTRE)
            tekst.SetForegroundColour(wx.WHITE)
            tekst.SetFont(wx.Font(48, wx.FONTFAMILY_SWISS, wx.BOLD, wx.NORMAL))
            return

        wx.StaticBitmap(self, -1, bilde, (self.x + 50, self.y))
        self.x = self.x + bilde.GetWidth() + 10

    """
	TODO: Make sure the entire handlekurv is comitted at once.
	"""

    def LagreKjop(self):
        good = True

        for vare in self.kurv:
            # Run the registerKryss method. This returns true on a sucessful query, and else false.
            good &= db.registerKryss(self.person, vare)

        # Legacy code. Store until new stuff is proven to work
        # try:
        #	vindu.db.query("INSERT INTO \"SociKryss\" (person,vare,kryssetid,pris) VALUES ('%s','%s',NOW(),'%d');" % (self.person['id'], k[0], k[1]))
        # except (pg.InternalError, pg.ProgrammingError):
        #	logger.error("klarte ikke lagre kryss på (%s) %s" %self.person)
        #	good = False
        #	self.GetParent().FjernHandlekurv(u'Klarte ikke lagre kryssene. Prøv igjen!', wx.RED, wx.BLACK)

        if good:
            self.GetParent().FjernHandlekurv('Kryssene er lagret', wx.GREEN, wx.BLACK)

    def Kanseller(self):
        self.GetParent().FjernHandlekurv('Kryssingen ble kansellert', wx.RED, wx.BLACK)


class Varer:
    def __init__(self, parent, id):
        self.varer = {}
        r = db.query("SELECT * FROM \"Varer\" WHERE bokstav IS NOT NULL;")

        if (r == None):
            return

        for v in r.dictresult():
            if len(glob.glob(BASE_DIR + 'varebilder/' + v['bildenavn'] + '.gif')) > 0:
                self.varer[v['bokstav']] = {'id': v['id'], 'navn': v['navn'], 'pris': v['pris'], 'bilde': wx.Image(
                    BASE_DIR + 'varebilder/' + v['bildenavn'] + '.gif').ConvertToBitmap()}

    def Get(self, key):
        return self.varer[key]

    def Test(self, key):
        for k, v in self.varer.iteritems():
            if (k == key):
                return True

        return False


logging.config.fileConfig('log/logging.conf')
logger = logging.getLogger('kismau')

# Start the database-connection
db = DbHandler()

# Start the Wx-application
app = wx.PySimpleApp()

# Create the main window-frame
vindu = Vindu(None, -1, 'Societeten')

# Create the varer-instance
varer = Varer(None, -1)

app.MainLoop()
