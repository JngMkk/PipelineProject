CREATE TABLE ciall (
    ciseq       INT NOT NULL,
    cicode      VARCHAR(20) NOT NULL,
    ciname      VARCHAR(255) NULL,
    cimingong   CHAR(10) NULL,
    ciinout     CHAR(10) NULL,
    ciplacecode VARCHAR(10) NULL,
    ciduty      CHAR(10) NULL,
    ciopercode  VARCHAR(10) NULL,
    ciaddr      VARCHAR(255) NULL,
    lat         DOUBLE NULL,
    lng         DOUBLE NULL,
    ctmnotidate CHAR(10) NULL,
    ctmreviewat CHAR(5) NULL,
    cifloorcode VARCHAR(10) NULL,
    PRIMARY KEY(ciseq, cicode)
);

CREATE TABLE ciaccident (
    cahseq      INT NOT NULL,
    cahdate     CHAR(10) NULL,
    cahcontent  VARCHAR(1000) NULL,
    cahaction   VARCHAR(1000) NULL,
    cahcode     VARCHAR(10) NULL,
    cicode      VARCHAR(20) NOT NULL,
    PRIMARY KEY(cahseq)
);

CREATE TABLE cicahcause (
    cahcode     VARCHAR(10) NOT NULL,
    cahcodeName VARCHAR(20) NOT NULL,
    PRIMARY KEY(cahcode)
);

CREATE TABLE cifloorinfo (
    cifloorcode VARCHAR(10) NOT NULL,
    flocodeName VARCHAR(10) NOT NULL,
    PRIMARY KEY(cifloorcode)
);

CREATE TABLE ciinst (
    caseq       INT NOT NULL,
    ciseq       INT NULL,
    cainstno    VARCHAR(10) NULL,
    cacuser     VARCHAR(20) NULL,
    cacdate     VARCHAR(10) NULL,
    ciinstcode  VARCHAR(10) NULL,
    ciinst      VARCHAR(30) NULL,
    cainstcode  VARCHAR(50),
    PRIMARY KEY(caseq)
);

CREATE TABLE cimanager (
    csseq       INT NOT NULL,
    ciseq       INT NULL,
    ciindgroup  VARCHAR(10) NULL,
    PRIMARY KEY(csseq)
);

CREATE TABLE cioperation (
    ciopercode  VARCHAR(10),
    opcodename  VARCHAR(10),
    PRIMARY KEY(ciopercode)
);

CREATE TABLE ciplaceinfo (
    ciplacecode VARCHAR(10),
    plcodename  VARCHAR(10),
    PRIMARY KEY(plcodename)
);