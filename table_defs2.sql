CREATE TABLE tickers(
    permaticker VARCHAR UNIQUE,
    ticker VARCHAR,
    name VARCHAR,
    exchange VARCHAR,
    isdelisted BOOLEAN,
    category VARCHAR,
    cusips VARCHAR,
    siccode VARCHAR,
    sicsector VARCHAR,
    sicindustry VARCHAR,
    famasector VARCHAR,
    famaindustry VARCHAR,
    sector VARCHAR,
    industry VARCHAR,
    scalemarketcap VARCHAR,
    scalerevenue VARCHAR,
    relatedtickers VARCHAR,
    currency VARCHAR,
    location VARCHAR,
    lastupdated DATE,
    firstadded DATE,
    firstpricedate DATE,
    lastpricedate DATE,
    firstquarter DATE,
    lastquarter DATE,
    secfilings VARCHAR,
    companysite VARCHAR,
    PRIMARY KEY (permaticker, ticker)
);

CREATE TABLE prices(
    permaticker VARCHAR,
    ticker VARCHAR,
    frequency VARCHAR CHECK (frequency = 'DAILY' OR frequency = 'MINUTE'),
    date TIMESTAMP,
    open NUMERIC,
    high NUMERIC,
    low NUMERIC,
    close NUMERIC,
    volume NUMERIC,
    closeadj NUMERIC,
    closeunadj NUMERIC,
    lastupdated DATE,
    PRIMARY KEY(ticker, date, frequency),
    FOREIGN KEY (permaticker, ticker) REFERENCES tickers (permaticker, ticker) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE fundamentals(
    revenue NUMERIC,
    cor NUMERIC,
    sgna NUMERIC,
    rnd NUMERIC,
    opex NUMERIC,
    intexp NUMERIC,
    taxexp NUMERIC,
    netincdis NUMERIC,
    consolinc NUMERIC,
    netincnci NUMERIC,
    netinc NUMERIC,
    prefdivis NUMERIC,
    netinccmn NUMERIC,
    eps NUMERIC,
    epsdil NUMERIC,
    shareswa NUMERIC,
    shareswadil NUMERIC,
    capex NUMERIC,
    ncfbus NUMERIC,
    ncfinv NUMERIC,
    ncff NUMERIC,
    ncfdebt NUMERIC,
    ncfcommon NUMERIC,
    ncfdiv NUMERIC,
    ncfi NUMERIC,
    ncfo NUMERIC,
    ncfx NUMERIC,
    ncf NUMERIC,
    sbcomp NUMERIC,
    depamor NUMERIC,
    assets NUMERIC,
    cashneq NUMERIC,
    investments NUMERIC,
    investmentsc NUMERIC,
    investmentsnc NUMERIC,
    deferredrev NUMERIC,
    deposits NUMERIC,
    ppnenet NUMERIC,
    inventory NUMERIC,
    taxassets NUMERIC,
    receivables NUMERIC,
    payables NUMERIC,
    intangibles NUMERIC,
    liabilities NUMERIC,
    equity NUMERIC,
    retearn NUMERIC,
    accoci NUMERIC,
    assetsc NUMERIC,
    assetsnc NUMERIC,
    liabilitiesc NUMERIC,
    liabilitiesnc NUMERIC,
    taxliabilities NUMERIC,
    debt NUMERIC,
    debtc NUMERIC,
    debtnc NUMERIC,
    ebt NUMERIC,
    ebit NUMERIC,
    ebitda NUMERIC,
    fxusd NUMERIC,
    equityusd NUMERIC,
    epsusd NUMERIC,
    revenueusd NUMERIC,
    netinccmnusd NUMERIC,
    cashnequsd NUMERIC,
    debtusd NUMERIC,
    ebitusd NUMERIC,
    ebitdausd NUMERIC,
    sharesbas NUMERIC,
    dps NUMERIC,
    sharefactor NUMERIC,
    marketcap NUMERIC,
    ev NUMERIC,
    invcap NUMERIC,
    equityavg NUMERIC,
    assetsavg NUMERIC,
    invcapavg NUMERIC,
    tangibles NUMERIC,
    roe NUMERIC,
    roa NUMERIC,
    fcf NUMERIC,
    roic NUMERIC,
    gp NUMERIC,
    opinc NUMERIC,
    grossmargin NUMERIC,
    netmargin NUMERIC,
    ebitdamargin NUMERIC,
    ros NUMERIC,
    assetturnover NUMERIC,
    payoutratio NUMERIC,
    evebitda NUMERIC,
    evebit NUMERIC,
    pe NUMERIC,
    pe1 NUMERIC,
    sps NUMERIC,
    ps1 NUMERIC,
    ps NUMERIC,
    pb NUMERIC,
    de NUMERIC,
    divyield NUMERIC,
    currentratio NUMERIC,
    workingcapital NUMERIC,
    fcfps NUMERIC,
    bvps NUMERIC,
    tbvps NUMERIC,
    price NUMERIC,
    permaticker VARCHAR,
    ticker VARCHAR,
    dimension VARCHAR,
    calendardate DATE,
    datekey DATE,
    reportperiod DATE,
    lastupdated DATE,
    PRIMARY KEY (ticker, dimension, datekey, reportperiod),
    FOREIGN KEY (permaticker, ticker) REFERENCES tickers (permaticker, ticker) DEFERRABLE INITIALLY DEFERRED
);

CREATE TABLE options(
    permaticker VARCHAR,
    ticker VARCHAR,
    date TIMESTAMP,
    strike INTEGER,
    side CHARACTER(1),
    bid NUMERIC,
    ask NUMERIC,
    impvol NUMERIC,
    delta NUMERIC,
    modelprice NUMERIC,
    gamma NUMERIC,
    vega NUMERIC,
    theta NUMERIC,
    underprice NUMERIC,
    PRIMARY KEY (ticker, date),
    FOREIGN KEY (permaticker, ticker) REFERENCES tickers (permaticker, ticker) DEFERRABLE INITIALLY DEFERRED
);