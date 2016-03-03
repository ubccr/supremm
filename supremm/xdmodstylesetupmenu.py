#!/usr/bin/env python
""" simple curses-based menu interface, intended to have a similar look and
    feel to the xdmod-setup menu in Open XDMoD
"""
import curses

class XDMoDStyleSetupMenu(object):
    """ Simple menu display interface, intended to have similar look and feel
    to the xdmod-setup menu for the Open XDMoD software """

    def __init__(self):
        self.stdscr = None
        self.row = 0
        self.page_title = ""

    def __enter__(self):
        """ init curses library """
        self.stdscr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.stdscr.keypad(1)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """ reset terminal settings and de-init curses """
        self.stdscr.keypad(0)
        curses.nocbreak()
        curses.echo()
        curses.endwin()

    def erase(self):
        """ Clear screen """
        self.stdscr.erase()
        self.row = 0

    def nextrow(self, increment=1):
        """ move down one row """
        maxy, _ = self.stdscr.getmaxyx()
        self.row = (self.row + increment) % maxy

    def print_text(self, text):
        """ Print multi-line text """
        for line in text.split("\n"):
            self.stdscr.addstr(self.row, 0, line)
            self.nextrow()

    def print_warning(self, text):
        """ Print multi-line text in bold font """
        for line in text.split("\n"):
            self.stdscr.addstr(self.row, 0, line, curses.A_STANDOUT)
            self.nextrow()

    def hitanykey(self, text):
        """ wait for the user to press the "any" key """
        self.stdscr.addstr(self.row, 0, text)
        self.nextrow()
        self.stdscr.getch()

    def prompt(self, text, options, default=None):
        """ Request input from user """

        self.stdscr.addstr(self.row, 0, text)
        self.stdscr.addstr(" (" + ",".join(options) + ") ")
        if default != None:
            self.stdscr.addstr("[" + default + "] ")

        self.nextrow()

        answer = None
        while answer not in options:
            ordch = self.stdscr.getch()
            if ordch == ord("\n") and default != None:
                answer = default
            elif ordch > 0 and ordch < 256:
                answer = chr(ordch)

        return answer

    def prompt_input(self, text, default):
        """ prompt user for input, preserving the datatype of the default value """
        if type(default) is bool:
            return self.prompt_bool(text, default)
        else:
            return self.prompt_string(text, default)

    def prompt_bool(self, text, default):
        """ prompt user to enter a boolean """
        defaultstr = "y" if default else "n"
        self.stdscr.addstr(self.row, 0, text)
        self.stdscr.addstr(" [" + defaultstr + "] ")
        self.nextrow()

        curses.echo()
        answer = self.stdscr.getstr()
        curses.noecho()

        if answer == "":
            retval = default
        else:
            answer = answer.lower()
            retval = answer.startswith("y") or answer.startswith("t")

        return retval

    def prompt_string(self, text, default):
        """ prompt user to enter text """
        self.stdscr.addstr(self.row, 0, text)
        if default != None:
            self.stdscr.addstr(" [" + str(default) + "] ")
        else:
            self.stdscr.addstr(" ")
        self.nextrow()

        curses.echo()

        answer = self.stdscr.getstr()
        if answer == "" and default != None:
            answer = default

        curses.noecho()

        return answer

    def prompt_password(self, text):
        """ prompt user to enter text """
        self.stdscr.addstr(self.row, 0, text + " ")
        self.nextrow()

        passwd = ""
        while True:
            userchar = self.stdscr.getkey()
            if userchar == "\n":
                return passwd
            else:
                passwd += userchar

    def newpage(self, title=None):
        """ Clear the screen and display a title. If the title is absent, then
            the previous title is reused.  """
        if title != None:
            self.page_title = title
        self.erase()
        self.stdscr.addstr(self.row, 0, self.page_title)
        self.nextrow()
        self.stdscr.move(self.row, 0)
        # pylint: disable=no-member
        self.stdscr.hline(curses.ACS_HLINE, len(self.page_title))
        # pylint: enable=no-member
        self.nextrow()

    def show_menu(self, title, items):
        """ Show a menu """

        done = False

        while not done:
            self.newpage(title)

            self.nextrow()
            for item in items:
                self.stdscr.addstr(self.row, 0, "  {0}) {1}".format(*item))
                self.nextrow()

            self.nextrow()
            self.stdscr.addstr(self.row, 0, "Select an item from the menu ")

            ordchar = self.stdscr.getch()

            if ordchar > 0 and ordchar < 256:
                for item in items:
                    if chr(ordchar) == item[0]:
                        if item[2] == None:
                            done = True
                        else:
                            item[2](self)
