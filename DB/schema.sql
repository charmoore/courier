-- Courier DB Schema
-- Version: 2.0
-- Author: Will Cassada
-- Email: will@cassidys.biz

CREATE TABLE IF NOT EXISTS providers (
	ProviderID int(10) NOT NULL AUTO_INCREMENT,
	Title varchar(8) NULL COMMENT 'MD, CRNA...',
	ProviderLast varchar(255) NOT NULL,
	ProviderFirst varchar(255) NOT NULL,
	ProviderMiddle varchar(255) NULL,
	ProviderEmail varchar(255) NULL,
	SurveyURL varchar(255) NOT NULL COMMENT "The URL to the provider's individual survey that will be sent to patients",
	PRIMARY KEY (ProviderID)
) ENGINE = InnoDB
	AUTO_INCREMENT = 1
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of providers and their info.  Updated periodically.  The General Practioner entry is the ';

CREATE TABLE IF NOT EXISTS plans (
	PlanID int(2) NOT NULL,
	PlanName varchar(50) NOT NULL,
	Description varchar(255) DEFAULT NULL,
	PRIMARY KEY (PlanID)
) ENGINE = InnoDB
	AUTO_INCREMENT = 1
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of plans that CLearSurvey sells.';

CREATE TABLE IF NOT EXISTS locations (
	LocationID int(4) NOT NULL AUTO_INCREMENT,
	LocationName varchar(255) NOT NULL,
	Address varchar(255) NULL,
	City varchar(100) DEFAULT NULL,
	State varchar(50) DEFAULT NULL,
	Country varchar(50) DEFAULT NULL,
	PRIMARY KEY (LocationID)
) ENGINE = InnoDB
	AUTO_INCREMENT = 1
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of locations where patients can be seen.';

CREATE TABLE IF NOT EXISTS patients (
	PatientID int(100) NOT NULL,
	Imported datetime NOT NULL COMMENT 'Datetime this patient was initially entered into the database.',
	Updated datetime NULL COMMENT 'Datetime this record was last updated.  Where applicable.',
	PatientLast varchar(255) NOT NULL,
	PatientFirst varchar(255) NOT NULL,
	PatientMiddle varchar(255) NULL,
	Age tinyint NOT NULL COMMENT 'Provided as an age.',
	Death date DEFAULT NULL COMMENT 'Date the patient expired.  If applicable.  Provided as DateOfDeath',
	Phone varchar(20) NULL,
	PhoneType int(2) NULL COMMENT 'The type of phone the patient has (e.g. Mobile, Landline). 1 is Mobile, 2 is Landline',
	Email varchar(100) NULL,
	OptOut tinyint DEFAULT 0 COMMENT 'Pinpoint has an opt out mechanism.  If the patient has chosen to opt out of messaging.  Record that response here.',
	PRIMARY KEY (PatientID)
) ENGINE = InnoDB
	AUTO_INCREMENT = 1
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table containing all patients that have been seen by providers using Clearsurvey.';

CREATE TABLE IF NOT EXISTS visits (
	VisitID varchar(50) NOT NULL COMMENT "Provided as SurveyRequestID",
	Updated datetime NULL COMMENT 'Datetime this record was last updated.  Where applicable.',
	PatientID int(10) NOT NULL COMMENT "Provided as PatientID",
	ProviderID int(10) NOT NULL COMMENT "ID of the Provider from the providers table",
	LocationID int(4) NOT NULL COMMENT "ID of the location from the location table",
	DateOfService date NOT NULL COMMENT "The date the patient was seen. Provided as DateOfService.",
	DatePosted date NOT NULL COMMENT "The date the visit was posted to the client's DB. Provided as PostedDate",
	VisitNumber varchar(255) DEFAULT NULL COMMENT "Provided as VisitNumber",
	UUID char(32) DEFAULT NULL COMMENT "Provided as UUID",
	Responded datetime NULL COMMENT "Date & time the patient responded to the survey",
	Reported date NULL COMMENT "Date this visit was reported back to the client/practice",
	Comments varchar(255) DEFAULT NULL COMMENT "Additional comments about this message.",
	PRIMARY KEY (VisitID),
	FOREIGN KEY (PatientID)
		REFERENCES patients(PatientID)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	FOREIGN KEY (ProviderID)
		REFERENCES providers(ProviderID)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	FOREIGN KEY (LocationID)
		REFERENCES locations(LocationID)
		ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE = InnoDB
	AUTO_INCREMENT = 1
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of patient visits imported from a client spreadsheet.';

CREATE TABLE IF NOT EXISTS reasons (
	ReasonID int(2) NOT NULL,
	Reason varchar(50) NOT NULL,
	Comments varchar(255) DEFAULT NULL COMMENT "Additional comments about the reason.",
	PRIMARY KEY (ReasonID)
) ENGINE = InnoDB
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of reasons messages could be sent or withheld.';

CREATE TABLE IF NOT EXISTS messageTypes (
	TypeID int(2) NOT NULL,
	Type varchar(50) NOT NULL,
	Comments varchar(255) DEFAULT NULL COMMENT "Additional comments about the type.",
	PRIMARY KEY (TypeID)
) ENGINE = InnoDB
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of message types that could be sent to a patient.';

CREATE TABLE IF NOT EXISTS messages (
	MessageID int(255) NOT NULL AUTO_INCREMENT,
	TypeID int(2) NOT NULL COMMENT 'The type of message sent.  E.g. initial, follow-up....',
	Updated datetime NOT NULL COMMENT 'Used to keep track of when the record in this table was processed, or updated.',
	DTGSent datetime COMMENT "Date and time the message was sent (or sent to pinpoint).",
	VisitID varchar(50) NOT NULL COMMENT "VisitID from visits table.",
	Sent tinyint DEFAULT 0 COMMENT "Was a message sent or not.",
	ReasonID int(2) NOT NULL COMMENT "Reason a message was sent or not. Lookup in reasons table.",
	SurveyLink varchar(255) NULL COMMENT 'The survey link included in this message.',
	Address varchar(255) NULL COMMENT 'The address (SMS or Email) the message was sent to.',
	Comments varchar(255) DEFAULT NULL COMMENT "Additional comments about this message.",
	PRIMARY KEY (MessageID),
	UNIQUE INDEX (MessageID, TypeID, ReasonID),
	FOREIGN KEY (VisitID)
		REFERENCES visits(VisitID)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	FOREIGN KEY (TypeID)
		REFERENCES messageTypes(TypeID)
		ON UPDATE RESTRICT ON DELETE RESTRICT,
	FOREIGN KEY (ReasonID)
		REFERENCES reasons(ReasonID)
		ON UPDATE RESTRICT ON DELETE RESTRICT
) ENGINE = InnoDB
	AUTO_INCREMENT = 1
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table for tracking clearsurvey messages sent';

CREATE TABLE IF NOT EXISTS responses (
	VisitID varchar(50) NOT NULL COMMENT "Provided as SurveyRequestID.  Used to keep track of patients and their responses to surveys",
	SurveyID varchar(255) NOT NULL COMMENT "Should match the survey idea from Modal Survey in the ...surveys table as (id).",
	DTG datetime COMMENT "The date and time the response was provided.  Pulled from the time field in the ...participants_details table in Model Survey.",
	QuestionID int(255) NOT NULL COMMENT "Pulled from Modal Survey",
	Question varchar(255) NOT NULL COMMENT "The individual question the patient responded to. This is a raw value (e.g. the exact text of the question).",
	AnswerID int(255) NOT NULL COMMENT "Pulled from Modal Survey",
	Answer varchar(255) NOT NULL COMMENT "The answer the patient provided to the given question.  This is a raw value (e.g. Great, Very Good).",
	Score int(2) NULL COMMENT "Value of the provided answer.",
	PRIMARY KEY (VisitID, QuestionID ),
	INDEX (SurveyID, AnswerID),
	FOREIGN KEY (VisitID)
		REFERENCES visits(VisitID)
		ON UPDATE CASCADE ON DELETE RESTRICT
) ENGINE = InnoDB
	DEFAULT CHARSET = utf8mb4
	COMMENT = 'Table of survey responses pulled from the WordPress Modal Survey databases.';

CREATE VIEW viewMessagesPending
	AS SELECT
		MessageID,
		messages.VisitID,
		messages.TypeID,
		reasons.Reason,
		DateOfService AS ServiceDate,
		patients.PatientID,
		CONCAT(PatientFirst, ' ', PatientLast) AS Patient,
		CONCAT(ProviderFirst, ' ', ProviderLast, ',', providers.Title) AS Provider,
		messages.SurveyLink AS Survey,
		messages.Address,
		OptOut,
		Age,
		DatePosted,
		Death,
		LocationName AS Location
	FROM messages
	INNER JOIN visits on messages.VisitID = visits.VisitID
	INNER JOIN patients on visits.PatientID = patients.PatientID
	INNER JOIN providers on visits.ProviderID = providers.ProviderID
	INNER JOIN locations on visits.LocationID = locations.LocationID
	JOIN reasons on messages.ReasonID = reasons.ReasonID
	WHERE messages.ReasonID = 1;

CREATE VIEW viewMessagesSent
	AS SELECT
		MessageID,
		messageTypes.Type,
		DTGSent AS DTG,
		messages.VisitID,
		DateOfService AS ServiceDate,
		CONCAT(PatientFirst, ' ', PatientLast) AS Patient,
		CONCAT(ProviderFirst, ' ', ProviderLast) AS Provider,
		messages.Address AS Address,
		messages.SurveyLink AS Survey,
		LocationName AS Location
	FROM messages
	INNER JOIN visits on messages.VisitID = visits.VisitID
	INNER JOIN patients on visits.PatientID = patients.PatientID
	INNER JOIN providers on visits.ProviderID = providers.ProviderID
	JOIN locations on visits.LocationID = locations.LocationID
	INNER JOIN messageTypes on messages.TypeID = messageTypes.TypeID
	WHERE Sent = 1;

CREATE VIEW viewMessagesNotSent
	AS SELECT
		MessageID,
		messageTypes.Type,
		DTGSent AS DTG,
		messages.VisitID,
		DateOfService AS ServiceDate,
		CONCAT(PatientFirst, ' ', PatientLast) AS Patient,
		CONCAT(ProviderFirst, ' ', ProviderLast) AS Provider,
		messages.Address AS Address,
		messages.SurveyLink AS Survey,
		LocationName AS Location
	FROM messages
	INNER JOIN visits on messages.VisitID = visits.VisitID
	INNER JOIN patients on visits.PatientID = patients.PatientID
	INNER JOIN providers on visits.ProviderID = providers.ProviderID
	JOIN locations on visits.LocationID = locations.LocationID
	INNER JOIN messageTypes on messages.TypeID = messageTypes.TypeID
	WHERE Sent = 0;

CREATE VIEW viewMessagesReport
	AS SELECT
		MessageID,
		DTGSent AS Sent,
		messages.VisitID,
		messages.TypeID AS Type,
		messages.ReasonID AS Reason,
		DateOfService AS ServiceDate,
		visits.Reported AS Reported
	FROM messages
	INNER JOIN visits on messages.VisitID = visits.VisitID
	WHERE visits.Reported is NULL
	ORDER BY Sent DESC;

/************************************************************************************
	Initial data
************************************************************************************/

INSERT INTO plans (PlanID, PlanName, Description) VALUES (1, 'ConnectSMS', 'Basic plan.  One text');
INSERT INTO plans (PlanID, PlanName, Description) VALUES (2, 'ConnectEMail', 'Basic plan.  One Email');
INSERT INTO plans (PlanID, PlanName, Description) VALUES (3, 'Connect', "Basic plan.  One text, or an email if phone doesn't support");
INSERT INTO plans (PlanID, PlanName, Description) VALUES (4, 'OtherOne', 'Send an SMS, follow-up with an SMS, follow-up with an e-mail.');
INSERT INTO plans (PlanID, PlanName, Description) VALUES (5, 'OtherTwo', 'Send an SMS, follow-up with an SMS, follow-up with an e-mail.');

INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (0, 'NULL', 'No message will be sent.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (1, 'Pending', 'Initial null message type.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (2, 'IniitalSMS', 'The initial message (SMS) sent to a patient.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (3, 'InitialEmail', 'The initial message (Email) sent to a patient.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (4, 'FollowupSMS', 'Follow-up message (Email) sent to a patient.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (5, 'FollowupEmail', 'Follow-up message (Email) sent to a patient.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (6, 'ResentSMS', 'A resend of the initial message (SMS) sent to a patient.');
INSERT INTO messageTypes (TypeID, Type, Comments) VALUES (7, 'ResentEmail', 'A resend of the initial message (Email) sent to a patient.');

INSERT INTO providers (ProviderID, Title, ProviderLast, ProviderFirst, ProviderMiddle, ProviderEmail, SurveyURL) VALUES (1, 'Provider', 'Provider', 'General', '', '', 'https://cpm.clearsurvey.com/gp');

INSERT INTO reasons (ReasonID, Reason, Comments) VALUES (1, 'Pending', 'Initial entry into the database.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (2, 'Sent', 'Used when the patient is not disqualified and will be/was sent a message.');
INSERT INTO reasons (ReasonID, reason, Comments) values (3, 'Under age', 'Used when the patient is under a specified minimum age (e.g. 18).');
INSERT INTO reasons (ReasonID, Reason, Comments) values (4, 'Deceased', 'Used when the patient is deceased.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (5, 'Phone blank', 'Used when the phone number provided is blank.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (6, 'Email Invalid', 'Used when the email address provided is invalid.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (7, 'Address Invalid', 'Used when the phone provided is too blank, long, short, or a landline.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (8, 'Opted out', 'Used when the patient has opted out of messaging.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (9, 'Segment pending', 'A segment was attempted in Pinpoint, but failed to create.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (10, 'Archive', 'Message not sent.  Visit occured prior to automated processing.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (11, 'Too old', 'Message not sent.  Date of service is greater than 30 days.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (12, 'Segment created', 'A segment has been created for this visit, but is pending processing by pinpoint.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (13, 'System failure.', 'An error occurred while processing this message.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (14, 'Campaign pending', 'A campaign was attempted in Pinpoint, but failed to create.');
INSERT INTO reasons (ReasonID, Reason, Comments) values (15, 'Campaign created', 'A campaign has been created for this visit, and is being processed by pinpoint.');
