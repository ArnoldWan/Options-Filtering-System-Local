CREATE TABLE Alpha_Vantage_API_Key_Usage_Count (
    Usage_ID INT PRIMARY KEY IDENTITY(1,1),
    API_Key_ID INT,
    API_KEY VARCHAR(255),
    Used_On_US_Date DATE,
    Count INT DEFAULT 1,
    Created_On DATETIME DEFAULT GETDATE(),
    Updated_On DATETIME,
    CONSTRAINT FK_APIKeyID FOREIGN KEY (API_Key_ID) REFERENCES Alpha_Vantage_API_Keys(API_Key_ID)
);