#! /usr/bin/env python
# coding: utf-8

## dbhandler.py

## Class the handle all database queries. As of March 2015 all db-queries were
## sort of spread around non-encapsulated. With the very ugly "Q" method
## shown below. This class is an attempt to encapsulate all database-queries in one class,
## and also allow us to implement real-time balance.

## Created by: Tormod Haugland
## Date: 10/03/2015
import wx, os, codecs, time, datetime, pg, glob, random, logging, logging.config, re, sys

import schedule, threading

from threading import Timer


class DbHandler:
    def __init__(self, root=None):

        # This should be a reference to the root-frame of the program. We use
        # this variable in case we need to kill the program
        self.root = root

        # Files
        logging.config.fileConfig('log/logging.conf')
        self.logger = logging.getLogger()

        # Initialize a lock. We need this to make sure our scheduled refresh-job doesn't interfer with any ongoing ope
        # We name this variable with an underscore to signify its importance!
        self.__lock = threading.Lock()

        # Connect to local and remote databases
        try:
            self.db_local = pg.DB(dbname='soci', host='localhost', port=5432, user='LOCAL_USER', passwd='LOCAL_PASSWORD')
        except pg.InternalError as error:
            self.logger.error("Fatal: Connection to local database failed: %s" % str(error))
            self.die()

        try:
            self.db_remote = pg.DB(dbname='sg', host='sql.samfundet.no', port=5432, user='REMOTE_USER', passwd='REMOTE_PASSWORD')
            print("Connected succesfully")
        except pg.InternalError as error:
            self.logger.error("Connection to remote database failed: %s" % str(error))
            self.logger.info("In absence of remote connection, the program is run in local-mode")
            self.local_mode = True
        else:
            self.local_mode = False

            # Array used to get the name of a month given an index 1-12. Our first value is empty as we will never have
        # to deal with month 0
        self.monthStringArray = ['', 'Januar', 'Februar', 'Mars', 'April', 'Mai', 'Juni', 'Juli', 'August', 'September',
                                 'Oktober', 'November', 'Desember']

        self.logger.info("DbHandler initialized and running")
        self.initInnkrysningsId()

        # Set up scheduled events with timer. Runs once every minute and checks for scheduled jobs.
        self.timer = Timer(60, self.timer_refresh)
        self.timer.start()

    # TODO - Reimplement scheduling

    def set_root(self, root):
        self.root = root

    def __del__(self):
        try:
            self.die()
            self.timer.cancel()
        except:
            # I am funny
            self.die()

    def timer_refresh(self):
        schedule.run_pending()
        self.timer = Timer(60, self.timer_refresh)
        self.timer.start()

    def initInnkrysningsId(self):
        """
        This function fetches the id of the relevant innkrysning. If no such innkrysning exists (i.e. we have changed months)
        we create a new one.
        """
        if self.local_mode:
            self.logger.warning("Innkryssing_id unable to be set while the program is running in localmode")
            return

        self.__lock.acquire()
        try:
            # We create this variable to erase the tiny tiny probability that the month changes during the execution
            # of this function
            date = self.db_local.query("SELECT NOW()::date;").getresult()[0][0]

            # Update currentMonthYear. Use the database to fetch the proper date to make sure the database is
            # satisfied I am not sure whether pythons date-fetches correlates 100% with psql's. Which is why this line
            #  of code exists.
            self.currentMonthYear = self.db_local.query("SELECT to_char(date('%s'), 'YYYY-MM');" % date).getresult()[0][
                0]

            # Check the local database for a Innkryss_id for this month
            innkryss_id = self.db_local.query(
                "SELECT innkryss_id FROM \"SociInnkryssId\" WHERE '%s' = to_char(dato, 'YYYY-MM');" % self.currentMonthYear)

            # If we can't find a previous DigiKryss-ID, we need to make a new one
            if (innkryss_id.ntuples() == 0):

                # Create a new Innkryss
                digikryss_navn = "Digikryss %s %s" % (self.monthStringArray[self.getMonth()], self.getYear())
                self.db_remote.query(
                    "INSERT INTO \"Innkryssinger\" (kryssetid, kommentar, lukket) VALUES (NOW(), '%s', 't');" % digikryss_navn)

                # Fetch the new id
                self.innkryss_id = \
                    self.db_remote.query("SELECT last_value FROM \"Innkryssinger_id_seq\";").getresult()[0][0]

                # Insert new id into local db
                self.db_local.query("INSERT INTO \"SociInnkryssId\" (innkryss_id, dato) VALUES ('%d', date('%s'));" % (
                    self.innkryss_id, date))
            else:
                msg = "Found previous innkryss for month/year: %s" % self.currentMonthYear
                print msg
                self.logger.info(msg)
                self.innkryss_id = innkryss_id.getresult()[0][0]
                print self.innkryss_id

        # If something went wrong
        except pg.InternalError as error:
            if (not self.restartConnections(True, True, False)):
                self.logger.error("Fatal: Connections crashed while initializing innkryss_id: %s" % str(error))
                self.die()
        finally:
            self.__lock.release()

    def refresh(self):
        """
        The program might run over month-changes, and as such we need to refresh it once in a while.
        We also check for lost kryss here. The method is scheduled to run once every 24 hours,
        """

        self.__lock.acquire()
        self.logger.info("Refreshing")
        # Re-Initializations
        try:
            if not self.testConnections():
                if not self.restartConnections(True, True, False):
                    self.logger.error("Fatal: Connections unable to be refreshed")
                    self.die()

            # Extract the year-month from the local database.
            monthYear = self.getMonthYearStamp()

            # Are we still in the same month?
            if monthYear != self.currentMonthYear:
                self.logger.info("Month has changed from %s to %s. Re-initializing innkryssing-id",
                                 (monthYear, self.currentMonthYear))
                self.initInnkrysningsId()
        # TODO: Can this even be thrown back here??
        except pg.InternalError as error:
            if (not self.testConnections()):
                if (not self.restartConnections()):
                    self.logger.error("Fatal: Unable to refresh program: Connections died.")
                    self.die()
        finally:
            # Check for lost kryss.
            self.checkLocalDatabase()
            self.__lock.release()

    """ Fetches the current month as an integer (1-12)"""

    def getMonth(self):
        return datetime.date.today().month

    """ Fetches the current year as an integer """

    def getYear(self):
        return datetime.date.today().year

    """ Fetches the current month and year on the format 'YYYY-MM' """

    def getMonthYearStamp(self):
        if (not self.testLocalConnection()):
            if (not self.restartConnections(True, False, False)):
                self.logger.error("Fatal: Connections unable to be refreshed while getMonthYearStamp")
                self.die()

        return self.db_local.query("SELECT to_char(NOW(), 'YYYY-MM');").getresult()[0][0]

    """
	Attempts to initiate connections to databses.

	Parameters:
		strict - If True the method will return True only if both connections were initiated succesfully.
				 If not the method will return True as long as the local connection was initiated succesfully.
	"""

    def startConnections(self, local=True, remote=True, strict=False):

        try:
            self.db_local = pg.connect('soci', 'localhost', 5432, None, None, 'soci', 'soci')
        except pg.InternalError as error:
            self.logger.error("Fatal: Connection to local database failed: %s" % str(error))
            return False

        try:
            self.db_remote = pg.connect('sg', 'sql.samfundet.no', 5432, None, None, 'REMOTE_USER', 'REMOTE_PASSWORD')
        except pg.InternalError as error:
            self.logger.error("Critical: Connection to remote database failed: %s" % str(error))
            self.logger.info("In absence of remote connection, the program is run in localmode")
            self.local_mode = True
            return True and not strict
        else:
            self.local_mode = False
            return True

    """
	Attempts to restart database connections.
	Will first check if connections are alive and close them if they are.

	Parameters:
		strict - If True the method will return True only if both connections were initiated succesfully.
				 If not the method will return True as long as the local connection was initiated succesfully.
	"""

    def restartConnections(self, local=True, remote=True, strict=False):
        if (self.testLocalConnection()):
            self.db_local.close()

        if (self.testRemoteConnection()):
            self.db_remote.close()

        return startConnections(local, remote, strict)

    """
	Refresh one or both of database connections. This method differs from "restartconnections" in
	that it tries first to reset the connections by calling db.reset(). If the local connection is unable to
	be refresh, the program dies. If the remote connection is unable to be refreshed, and the local connection
	has died in the meanwhile, we kill the program. If only the local connection is able to be refreshed, we
	enter localmode.
	"""

    def refreshConnections(self, local=True, remote=True):

        # Set it to false until otherwise noticed.
        self.local_mode = False

        if (local):
            self.db_local.reset()

            if (not self.testLocalConnection()):
                self.die()

        if (remote):
            self.db_remote.reset()

            if (not self.testRemoteConnection()):
                if (not self.testLocalConnection()):
                    self.logger.error("Fatal: Connection to both remote and local database has died. Shutting down...")
                    self.die()
                else:
                    self.logger.info("As remote has died, we are going over to local mode.")
                    self.local_mode = True

    """ Test if both database-connections are alive """

    def testConnections(self):
        return self.testLocalConnection() and self.testRemoteConnection()

    """ Check if the connection to the local database is alive """

    def testLocalConnection(self):
        try:
            self.db_local.query("SELECT TRUE")
        except:
            self.logger.error("Fatal: Connection to local database has died: %s " % str(error))
            return False
        else:
            return True

    """ Check if the connection to the remote database is alive """

    def testRemoteConnection(self):
        try:
            self.db_local.query("SELECT TRUE")
        except:
            self.logger.error("Fatal: Connection to remote database has died: %s " % str(error))
            return False
        else:
            return True

    """
	Kill the DbHandler, and with it the program
	"""

    def die(self, args=None):
        self.timer.stop()

        # If we have a window registered with the DbHandler, kill it.
        if (self.root):
            self.root.Close(True)
        else:
            self.logger.info(
                "DbHandler is missing a root-window, and is as such unable to close the program. Is it set properly?")

        self.db_local.close()
        self.db_remote.close()

        # Do this in case some extending entity has decided to use the same lock. (Oh how stupid aren't thou if thou has done this?)
        self.__lock.release()

    def registerKryss(self, person, vare, registerLocal=True, registerRemote=True, tryLocalOnRemoteFail=True):
        if (self.local_mode and local):
            return self.registerKryssLocalMode(self, person, vare)

        if (registerLocal):
            try:
                self.db_local.query(
                    "INSERT INTO \"SociKryss\" (person, vare, kryssetid, antall, pris, overfort) VALUES ('%s', '%s', NOW(), '%d', '%d', 't')" % (
                        person['id'], vare[0], vare[1], vare[2]))
            except Exception as err:
                print str(err)
                self.logger.error(str(err))
                self.registerKryssOnDbFailure(person, vare)
                return False

        if (registerRemote):
            try:
                # If we have no innkryss_id and need to initialize one.
                if (not self.innkryss_id):
                    self.initInnkrysningsId()

                self.db_remote.query("BEGIN;")
                self.db_remote.query(
                    "INSERT INTO \"Kryss\" (innkryssing, person, vare, kryssetid, antall, pris) VALUES ('%d', '%d', '%d', NOW(), '%d', '%d');" % (
                        self.innkryss_id, person['id'], vare[0], vare[1], vare[2]))

            except (ValueError, pg.Error) as error:
                # Issue a rollback to the remote database.
                self.logger.error("Error while registering kryss with the remote database: %s" % str(error))
                self.db_remote.query("ROLLBACK;")

                if (not tryLocalOnRemoteFail):
                    return False
                else:
                    self.local_mode = True
                    self.logger.warning("Unable to register kryss with remote database, changing to local mode")
                    return registerKryssLocalMode(self, person, vare)
            else:
                self.db_remote.query("COMMIT;")
                return True

    """
	Attempts to register a kryss that has been stored locally for backup to the remote database.

	Parameters:
		innkryss_id: We here take an innkryss_id as a parameter, as we want to store our missing kryss in a seperate innkryss.
					 The reason for this is that we might want to have visible to us at a later point which kryss where missing.
	"""

    def registerMissingKryss(self, innkryss_id, person, vare):

        try:
            self.db_remote.query("BEGIN;")
            self.db_remote.query(
                "INSERT INTO \"Kryss\" (innkryssing, person, vare, kryssetid, antall, pris) VALUES ('%d', '%d', '%d', '%s', '%d', '%d');" % (
                    innkryss_id, person['id'], vare[0], vare[1], vare[2], vare[3]))

        except (ValueError, pg.Error) as error:
            # Issue a rollback
            self.logger.error("Error while registering lost kryss to the remote database")
            self.db_remote.query("ROLLBACK;")
        else:
            self.db_remote.query("COMMIT;")
            return True

    """
	Register a kryss to the local back-up database (and the standard database).
	It is important that it is refrained from calling this method unless we are in local mode.
	In all other circumstances a kryss will be registered twice.

	Paramters:
		force: Will run the method even if we are not in localmode. BEWARE OF USING THIS!
			   If not used properly, this will cause duplicates
	"""

    def reigsterKryssLocalMode(self, person, vare, force=False):

        self.__lock.acquire()

        if (not self.local_mode and not force):
            self.logger.warning("registerKryssLocalMode was called without localmode being set.")
            return

        try:
            # Register the kryss with the local "all-database"
            self.db_local.query(
                "INSERT INTO \"SociKryss\" (person, vare, kryssetid, pris) VALUES ('%s', '%s', NOW(), '%d')" % (
                    person['id'], vare[0], vare[1]))

            # Register the kryss with the local database used for lost kryss
            # self.db_remote.query("INSERT INTO \"SociLocalKryss\" (person, vare, kryssetid, pris) VALUES ('%s', '%s', NOW(), '%d')" % (person['id'], vare[0], vare[1]))
            return True
        except Exception as err:
            # TODO: Should we crash here?
            self.logger.error("Unable to write to local database while in localmode. This is bad ...")
        finally:
            self.__lock.release()

    """
	Fetches info of a person based on the card-number.
	"""

    def getPersonFromCard(self, card):

        self.__lock.acquire()

        if (self.local_mode):
            return self.getPersonFromCardLocal(card)

        # Try to fetch from remote database
        try:
            person = self.db_remote.query(
                "SELECT id, navn, saldo FROM \"Personer\" WHERE TRIM(kortnummer) = '%s'" % card)
            if (person.ntuples() == 0):
                return None
            else:
                return person.dictresult()[0]

        # If anything went wrong we try to fetch the person from the local database
        except Exception as err:
            self.logger.error(
                "Unable to retreieve person info for card-number %s from remote database: %s. Attempting local mode." % (
                    str(err), card))

            personFromLocal = self.getPersonFromCardLocal(card, True)
            if (not personFromLocal):
                self.logger.error(
                    "Unable to retrieve person info for card-number %s from local and remote database." % card)
                return None
            else:
                return personFromLocal
        finally:
            self.__lock.release()

    """
	Fethces info of a person based on the card-number while in localmode.

	Parameters:
		force: If this is set to true, the method will be run regardless of whether we are in localmode or not.
	"""

    def getPersonFromCardLocal(self, card, force=False):
        if (not self.local_mode and not force):
            self.logger.warning("registerKryssLocalMode was called without localmode being set.")
            return

        try:
            person = self.db_local.query(
                "SELECT id, navn, saldo FROM \"SociPersoner\" WHERE TRIM(kortnummer) = '%s'" % card)

            if (person.ntuples() == 0):
                return None
            else:
                return person.dictresult()[0]

        except Exception as err:
            self.logger.error(
                "Unable to retrieve person info for card-number %s from local database while in localmode." % card)
            return None

    """
	Check local database for pending transfers that was not registered.
	TODO
	If any such transfers are found, try to preform transfers to remote database.
	"""

    def checkLocalDatabase(self):
        try:
            # All kryss that are stored in the table SociLocalKryss
            local_kryss = self.db_local.query(
                "SELECT id, person, vare, kryssetid, pris FROM \"SociKryss\" WHERE overfort = false").dictresult()

            print type(local_kryss)
            if (len(local_kryss) > 0):
                date = self.db_local.query("SELECT NOW()::date;").getresult()[0][0]

                # Init new innkryss for tapte kryss
                lostkryss_navn = "TapteKryss %s" % date
                self.db_remote.query(
                    "INSERT INTO \"Innkryssinger\" (kryssetid, kommentar, lukket) VALUES ('NOW()', '%s', 't');" % lostkryss_navn)

                # Fetch the new id
                innkryss_id = self.db_remote.query("SELECT last_value FROM \"Innkryssinger_id_seq\";").getresult()[0][0]

                for kryss in local_kryss:
                    # If we manage to register the kryss to the remote databse sucessfully
                    if (self.registerMissingKryss(innkryss_id, {'id': kryss['person']},
                                                  (kryss['kryssetid'], kryss['vare'], 1, kryss['pris']))):
                        print "Hello"
                        try:
                            self.db_local.query("BEGIN;")
                            self.db_local.query("UPDATE \"SociKryss\" SET overfort = true WHERE id = %d" % kryss['id'])
                        except Exception as err:
                            self.logger.error(
                                "Unable to update \"SociKryss\" to overfort = true: person=%d, kryss=%d.\nPlease register manually." % (
                                    kryss['person'], kryss['id']))
                        else:
                            self.db_local.query("COMMIT;")
                self.logger.info("Updated external database with %d kryss from local database." % len(local_kryss))

        except Exception as err:
            self.logger.error("Unable to check local databse: %s" % str(err))

    def query(self, query):
        try:
            return self.db_remote.query(query)
        except Exception as err:
            self.logger.error("Unable to perform query: %s with error %s" % (query, str(err)))
