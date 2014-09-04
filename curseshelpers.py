from __future__ import division
import curses
import time
import math
def default(x, y):
    if x is not None: return x
    return y
def chunks(l, n):
    """ Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

class Text:
    def __init__(self, screen, maxrows=200, top=1, left=0, height=None, width=None):
        self.parent = screen
        self.top, self.left = top, left
        self._height = default(height, screen.getmaxyx()[0]-top)
        self._width = default(width, screen.getmaxyx()[1])
        self.pad = curses.newpad(maxrows, self.width)
        self.position = 0
        self.text = ""
        self.nrows=0
    def readFile(self, filename):
        f = open(filename, 'r')
        self.setText(f.read())
        f.close()
    def setText(self, text):
        self.text=text
        self._redraw()
    def refresh(self):
        self.pad.refresh(self.position, 0, self.top, self.left, self.top+self.height-1, self.left+self.width-1)
    def goUp(self):
        self.position=max(0, self.position-1)
    def goDown(self):
        self.position=min(self.position+1, self.nrows-self.height)
    def pageUp(self):
        pass
    def pageDown(self):
        pass
    def _redraw(self):
        rows=[]
        for line in self.text.splitlines():
            rows+=list(chunks(line,self.width))
        for (row, rownumber) in zip(rows, range(len(rows))):
            self.pad.addstr(rownumber, 0, row)
        self.nrows=len(rows)
    #def pageUp(self):
    #def pageDown(self):
    def home(self):
        self.position = 0
    def end(self):
        self.position = self.nrows-self.height
    @property
    def height(self):
        return min(self._height,self.parent.getmaxyx()[0]-self.top-1)
    @property
    def width(self):
        return min(self._width, self.parent.getmaxyx()[1])

class SelectTable:
    def __init__(self, screen, maxrows=200, top=1, left=0, height=None, width=None):
        self.parent = screen
        self.top, self.left = top+1, left
        self._height = default(height, screen.getmaxyx()[0]-top)-1
        self._width = default(width, screen.getmaxyx()[1]-1)
        self.pad = curses.newpad(maxrows, self.width)
        self.header = curses.newwin(1, self.width, top, left)
        self.cursor, self.position = 0, 0
        self.rows, self.formats, self.keys = [], [], []
    def setColHeaders(self, headers, colwidths=None):
        self.colHeaders = headers
        if colwidths is None:
            x=int(math.floor(self.width/len(self.colHeaders)))-1
            self.colWidths = [x]*len(self.colHeaders)
        else:
            self.colWidths = colwidths
        self._drawColHeaders()
    def _drawColHeaders(self):
        for i in range(len(self.colHeaders)):
            try:
                self.header.addstr(0, sum(self.colWidths[:i]), ("{0:^"+str(self.colWidths[i])+"."+str(self.colWidths[i]-1)+"}").format(self.colHeaders[i]), curses.A_UNDERLINE)
            except:
                raise Exception(str(self.colWidths)+", "+str(("{0:^"+str(self.colWidths[i]-1)+"."+str(self.colWidths[i]-1)+"} ").format(self.colHeaders[i])))
        self.header.addstr(0, sum(self.colWidths), " "*(self.width-sum(self.colWidths)-1), curses.A_UNDERLINE)
        
    def addRow(self, row, formatting=None, key=None):
        self.rows.append(row)
        self.formats.append(default(formatting,curses.A_NORMAL))
        self.keys.append(default(key, self.nrows))
        self._redrawRows(self.nrows-1)
    def clear(self):
        self.rows, self.formats, self.keys = [], [], []
    def refresh(self):
        self.pad.refresh(self.position, 0, self.top, self.left, self.top+self.height-1, self.left+self.width-1)
        self._drawColHeaders()
        self.header.refresh()
    def goUp(self):
        oldcursor = self.cursor
        self.cursor=max(self.cursor-1, 0)
        self._redrawRows(self.cursor, oldcursor)
        if self.cursor-self.position<1/4*self.height and self.position>0:
            self.position -= 1
    def goDown(self):
        oldcursor = self.cursor
        self.cursor=min(self.cursor+1, self.nrows-1)
        self._redrawRows(self.cursor, oldcursor)
        #if self.position<self.cursor-2/3*self.height and self.position+self.height<self.nrows:
        if self.cursor-self.position>3/4*self.height and self.position+self.height<self.nrows:
            self.position += 1
    def _redrawRows(self, *rowids):
        for rownumber in rowids:
            if rownumber==self.cursor:
                formatting = self.formats[rownumber] | curses.A_REVERSE
            else:
                formatting=self.formats[rownumber]
            for cell, i in zip(self.rows[rownumber], range(len(self.rows[rownumber]))):
                if isinstance(cell, (int, long, float, complex)):
                    direction=">"
                else:
                    direction="<"
                #self.pad.addstr(rownumber, sum(self.colWidths[:i]), str(cell)[0:self.colWidths[i]-1], formatting)
                self.pad.addstr(rownumber, sum(self.colWidths[:i]), ("{0:"+direction+str(self.colWidths[i]-1)+"."+str(self.colWidths[i]-1)+"} ").format(str(cell)), formatting)
            self.pad.addstr(rownumber, sum(self.colWidths), " "*(self.width-sum(self.colWidths)-1), formatting)
    @property
    def nrows(self):
        return len(self.rows)
    def pageUp(self):
        oldcursor = self.cursor
        self.cursor=max(self.cursor-self.height, 0)
        self.position=max(self.position-self.height, 0)
        self._redrawRows(self.cursor, oldcursor)
    def pageDown(self):
        oldcursor = self.cursor
        self.cursor=min(self.cursor+self.height, self.nrows-1)
        self.position=min(self.position+self.height, self.nrows-self.height)
        self._redrawRows(self.cursor, oldcursor)
    def home(self):
        oldcursor = self.cursor
        self.position, self.cursor = 0, 0
        self._redrawRows(self.cursor, oldcursor)
    def end(self):
        oldcursor = self.cursor
        self.position, self.cursor = self.nrows-self.height, self.nrows-1
        self._redrawRows(self.cursor, oldcursor)
    @property
    def selectedRow(self):
        return self.keys[self.cursor]
    @property
    def height(self):
        return min(self._height,self.parent.getmaxyx()[0]-self.top-1)
    @property
    def width(self):
        return min(self._width, self.parent.getmaxyx()[1])
        
        
def main(stdscr):
    curses.noecho()
    curses.curs_set(0)
    stdscr.keypad(1)
    table = SelectTable(stdscr)
    table.setColHeaders(["Spaltennamensuper","Spalte 2", "Spalte 3", "Spalte 4"],[10,10,10,10])
    for i in range(100):
        table.addRow([i,"zwei","drei","vier"])
    #table = Text(stdscr)
    #table.readFile("testmenu.py")
    while True:
        stdscr.refresh()
        table.refresh()
        x = stdscr.getch()
        if x==ord("i"):
            table.goUp()
        if x==ord("j"):
            table.goDown()
        if x==ord("k"):
            table.home()
        if x==ord("l"):
            table.end()
        if x==ord("q"):
            break
        
if __name__ == "__main__":
    curses.wrapper(main)
