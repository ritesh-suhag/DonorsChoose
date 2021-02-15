library("tidyverse")
library("dplyr")
library("tidyr")

#x <- (table(Donors$`Donor ID`))
#x[(x$Freq > 1), ]  

# TEACHERS - 
#Creating a numeric ID for teachers
Teachers$Teacher_ID <- c(1:length(Teachers$`Teacher ID`))
#Removing the essay column, as it's not required for us.
Projects$`Project Essay` <- NULL
#Joining teachers with Project table as Teacher_ID has foreign key in the Projects table.
#This helps to map character_ID of Teacher to numeric_ID in project table
Projects_1 <- left_join(Projects, Teachers, by = "Teacher ID")
#Removing unwanted tables created due to join in R.
Projects_1$`Teacher ID` <- NULL
Projects_1$`Teacher First Project Posted Date` <- NULL
Projects_1$`Teacher Prefix` <- NULL
Teachers_1 <- Teachers
Teachers_1$`Teacher ID` <- NULL
# Make New Table Final_Teachers to make columns in order.---------------
Teachers_Final <- data.frame(Teacher_ID = Teachers_1$Teacher_ID, 
                             Prefix = Teachers_1$`Teacher Prefix`, 
                             First_Post_Date = Teachers_1$`Teacher First Project Posted Date`)
#Saving the final table in local system
write.table(x = Teachers_Final, file = "Teachers_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Teachers_1)
#Making a new table for state. Creating numeric ID.
State_1 <- data.frame(table(Donors$`Donor State`))
State_1$State_Id <- c(1:52)
#Makig final table according to the desired column sequence 
State_Final <- data.frame(State_ID = State_1$State_Id, State = State_1$Var1)
#Saving the final table in local system
write.table(x = State_Final[,c(1:2)], file = "State_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables
rm(State_1)
#DONORS -----------------------
Donors_1 <- Donors
#Mapping the state numeric ID from state table.
Donors_1$State <- Donors_1$`Donor State`
Donors_1 <- left_join(Donors_1, State_Final, by ="State")
Donors_1$`Donor State` <- NULL
#Creating numeric Donors_ID
Donors_1$Donor_ID <- c(1:2122640)
#Joining with Donations table as Donations has Donor as foreign key.
Donations_1 <- left_join(Donations, Donors_1, by = "Donor ID")
#Removing unwanted tables created due to join in R.
Donations_1$State_ID <- NULL
Donations_1$State <- NULL
Donations_1$`Donor Zip` <- NULL
Donations_1$`Donor Is Teacher` <- NULL
Donations_1$`Donor City` <- NULL
Donations_1$`Donor ID` <- NULL
Donors$`Donor State` <- NULL
Donors_1$State  <- NULL
Donors_1$`Donor ID` <- NULL
#Makig final table according to the desired column sequence 
Donors_Final <- data.frame(Donor_Id = Donors_1$Donor_ID, 
                           Donor_City = Donors_1$`Donor City`, 
                           Donor_Is_Teacher = Donors_1$`Donor Is Teacher`, 
                           Donor_ZIP <- Donors_1$`Donor Zip`, 
                           State_ID <- Donors_1$State_ID)
#Saving the final table in local system
write.table(x = Donors_Final, file = "Donors_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Donors_1)
#SCHOOLS ----------------
Schools_1 <- Schools
#Mapping the state numeric ID from state table.
Schools_1$State <- Schools_1$`School State`
Schools_1 <- left_join(Schools_1, State_Final, by ="State")
#Removing unwanted tables created due to join in R.
Schools_1$State <- NULL
Schools_1$`School State` <- NULL
#Making a different table for Metro_Type.
School_Metro_Type <- data.frame(table(Schools_1$`School Metro Type`))
#Creating numeric ID for Metro_Type.
School_Metro_Type$Metro_Type_ID <- c(1:5)
Schools_1$Metro_Type <- Schools_1$`School Metro Type`
School_Metro_Type$Metro_Type <- School_Metro_Type$Var1
#Mapping the numeric ID of MEtro_Type to the Schools table
Schools_1 <- left_join(Schools_1, School_Metro_Type, by = "Metro_Type")
#Removing unwanted tables created due to join in R.
Schools_1$Freq <- NULL
Schools_1$Var1 <- NULL
Schools_1$Metro_Type <- NULL
Schools_1$`School Metro Type` <- NULL
#Makig final table according to the desired column sequence 
School_Metro_Type_Final <- data.frame(Metro_Type_Id <- School_Metro_Type$Metro_Type_ID, 
                                      Metro_Type = School_Metro_Type$Metro_Type)
#Saving the final table in local system
write.table(x = School_Metro_Type_Final, file = "School_Metro_Type_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(School_Metro_Type)
#Creating numeric School_ID
Schools_1$School_ID <- c(1:72993)
#Joining with Project table as Project has School as foreign key.
Projects_1 <- left_join(Projects_1, Schools_1, by = "School ID")
#Removing unwanted tables created due to join in R.
Projects_1$Metro_Type_ID <- NULL
Projects_1$State_ID <- NULL
Projects_1$`School District` <- NULL
Projects_1$`School County` <- NULL
Projects_1$`School City` <- NULL
Projects_1$`School Zip` <- NULL
Projects_1$`School Percentage Free Lunch` <- NULL
Projects_1$`School Name` <- NULL
Projects_1$`School ID` <- NULL
Schools_1$`School ID` <- NULL
#Makig final table according to the desired column sequence 
Schools_Final <- data.frame(School_Id = Schools_1$School_ID, 
                            School_Name = Schools_1$`School Name`,
                            Metro_Type_ID = Schools_1$Metro_Type_ID, 
                            Percentage_Free_Lunch = Schools_1$`School Percentage Free Lunch`, 
                            ZIP = Schools_1$`School Zip`, 
                            City = Schools_1$`School City`, 
                            County = Schools_1$`School County`, 
                            District = Schools_1$`School District`, 
                            State_ID = Schools_1$State_ID)
#Saving the final table in local system
write.table(x = Schools_Final, file = "School_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Schools_1)

#----------------------- Projects
Projects_Clean <- data.frame(table(Projects_1$`Project ID`))
#There were repeated PK valyues in Project_ID, removing those (only 4 were repeated)
Not_Req <- Projects_Clean[Projects_Clean$Freq > 1,]
Not_Req
which(grepl("c940d0e78b7559573aca536db90c0646", Projects_1$`Project ID`))
which(grepl("99c07777fdcf63d3a0fdb4a0deb4b012", Projects_1$`Project ID`))
Projects_1 <- Projects_1[-c(983394,983536,1041064,1041434),]
Projects_1$Project_ID <- c(1:1110013)
rm(Projects_Clean)
rm(Not_Req)
#Making a different table for Subject category
Projects_1 <- Projects_1 %>% separate(`Project Subject Category Tree`, 
                                      c("Subject_Category_1","Subject_Category_2","Subject_Category_3"), ",")
x <- unique(Projects_1$Subject_Category_1)
y <- unique(Projects_1$Subject_Category_2)
z <- unique(Projects_1$Subject_Category_3)
Subject_Category <- data.frame(Subject_Category = unique(c(x,y,z)))
rm(x)
rm(y)
rm(z)
#Creating numeric ID for Subject category.
Subject_Category$Subject_Category_ID <- c(1:18)
#Makig final table according to the desired column sequence 
Subject_Category_Final <- data.frame(Subject_Category_ID = Subject_Category$Subject_Category_ID, 
                                     Subject_Category = Subject_Category$Subject_Category)
#Saving the final table in local system
write.table(x = Subject_Category_Final, file = "Subject_Category_Final.csv", sep =',', row.names = FALSE)
#Mapping the numeric ID of Subject category to the Project table columns - CSubject ategory 1,2,3.
Projects_1$Subject_Category <- Projects_1$Subject_Category_1
Projects_1 <- left_join(Projects_1, Subject_Category, by = "Subject_Category")
Projects_1$Subject_Category_ID_1 <- Projects_1$Subject_Category_ID
#Removing unwanted tables created due to join in R.
Projects_1$Subject_Category_1 <- NULL
Projects_1$Subject_Category <- NULL
Projects_1$Subject_Category_ID <- NULL
Projects_1$Subject_Category <- Projects_1$Subject_Category_2
Projects_1 <- left_join(Projects_1, Subject_Category, by = "Subject_Category")
Projects_1$Subject_Category_ID_2 <- Projects_1$Subject_Category_ID
#Removing unwanted tables created due to join in R.
Projects_1$Subject_Category_2 <- NULL
Projects_1$Subject_Category <- NULL
Projects_1$Subject_Category_ID <- NULL
Projects_1$Subject_Category <- Projects_1$Subject_Category_3
Projects_1 <- left_join(Projects_1, Subject_Category, by = "Subject_Category")
Projects_1$Subject_Category_ID_3 <- Projects_1$Subject_Category_ID
#Removing unwanted tables created due to join in R.
Projects_1$Subject_Category_3 <- NULL
Projects_1$Subject_Category <- NULL
Projects_1$Subject_Category_ID <- NULL
rm(Subject_Category)
#Making a different table for Project Type
Project_Type <- data.frame(table(Projects_1$`Project Type`))
Project_Type$Project_Type <- Project_Type$Var1
Project_Type$Var1 <- NULL
#Creating Numeric ID for project_Type
Project_Type$Project_Type_ID <- c(1:3)
Project_Type$Freq <- NULL
#Makig final table according to the desired column sequence 
Project_Type_Final <- data.frame(Project_Type_ID = Project_Type$Project_Type_ID, 
                                 Project_Type = Project_Type$Project_Type)
Projects_1$Project_Type <- Projects_1$`Project Type`
#Mapping the numeric ID of Project Type to the Project table
Projects_1 <- left_join(Projects_1, Project_Type, by = "Project_Type")
#Removing unwanted tables created due to join in R.
Projects_1$Project_Type <- NULL
Projects_1$`Project Type` <- NULL
#Saving the final table in local system
write.table(x = Project_Type_Final, file = "Project_Type_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Project_Type)
#Making a different table for Grade Level
Grade_Level_Category <- data.frame(table(Projects_1$`Project Grade Level Category`))
#Creating Numeric ID for Grade_Level
Grade_Level_Category$Grade_Level_Category_ID <- c(1:5)
Grade_Level_Category$Grade_Level_Category <- Grade_Level_Category$Var1
Grade_Level_Category$Var1 <- NULL
Grade_Level_Category$Freq <- NULL
Projects_1$Grade_Level_Category <- Projects_1$`Project Grade Level Category`
#Mapping the numeric ID of Grade_Level to the Project table
Projects_1 <- left_join(Projects_1, Grade_Level_Category, by = "Grade_Level_Category")
#Removing unwanted tables created due to join in R.
Projects_1$`Project Grade Level Category` <- NULL
Projects_1$Grade_Level_Category <- NULL
#Makig final table according to the desired column sequence 
Grade_Level_Category_Final <- data.frame(Grade_Level_Category_ID = Grade_Level_Category$Grade_Level_Category_ID,
                                         Grade_Level_Category = Grade_Level_Category$Grade_Level_Category)
#Saving the final table in local system
write.table(x = Grade_Level_Category_Final, file = "Grade_Level_Category_Final.csv", 
            sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Grade_Level_Category)
#Separating Subject Sub Category Into 3 columns
Projects_1 <- Projects_1 %>% separate(`Project Subject Subcategory Tree`, 
                                      c("Subject_Sub_Category_1","Subject_Sub_Category_2",
                                        "Subject_Sub_Category_3")
                                      ,",")
x <- unique(Projects_1$Subject_Sub_Category_1)
y <- unique(Projects_1$Subject_Sub_Category_2)
z <- unique(Projects_1$Subject_Sub_Category_3)
Subject_Sub_Category <- data.frame(Subject_Sub_Category = unique(c(x,y,z)))
rm(x)
rm(y)
rm(z)
#Making the numeric ID of Subject Sub Category
Subject_Sub_Category$Subject_Sub_Category_ID <- c(1:59)
Projects_1$Subject_Sub_Category <- Projects_1$Subject_Sub_Category_1
Projects_1 <- left_join(Projects_1, Subject_Sub_Category, by = "Subject_Sub_Category")
Projects_1$Subject_Sub_Category_ID_1 <- Projects_1$Subject_Sub_Category_ID
Projects_1$Subject_Sub_Category_1 <- NULL
Projects_1$Subject_Sub_Category <- NULL
Projects_1$Subject_Sub_Category_ID <- NULL
Projects_1$Subject_Sub_Category <- Projects_1$Subject_Sub_Category_2
Projects_1 <- left_join(Projects_1, Subject_Sub_Category, by = "Subject_Sub_Category")
Projects_1$Subject_Sub_Category_ID_2 <- Projects_1$Subject_Sub_Category_ID
Projects_1$Subject_Sub_Category_2 <- NULL
Projects_1$Subject_Sub_Category <- NULL
Projects_1$Subject_Sub_Category_ID <- NULL
Projects_1$Subject_Sub_Category <- Projects_1$Subject_Sub_Category_3
Projects_1 <- left_join(Projects_1, Subject_Sub_Category, by = "Subject_Sub_Category")
Projects_1$Subject_Sub_Category_ID_3 <- Projects_1$Subject_Sub_Category_ID
Projects_1$Subject_Sub_Category_3 <- NULL
Projects_1$Subject_Sub_Category <- NULL
Projects_1$Subject_Sub_Category_ID <- NULL
#Makig final table according to the desired column sequence 
Subject_Sub_Category_Final <- data.frame(Subject_Sub_Category_ID = Subject_Sub_Category$Subject_Sub_Category_ID,
                                         Subject_Sub_Category = Subject_Sub_Category$Subject_Sub_Category)
#Saving the final table in local system
write.table(x = Subject_Sub_Category_Final, file = "Subject_Sub_Category_Final.csv", 
            sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Subject_Sub_Category)
#Making a different table for Resource Category
Resource_Category <- data.frame(table(Projects_1$`Project Resource Category`))
#Making the numeric ID of Resource Category
Resource_Category$Resource_Category_ID <- c(1:17)
Resource_Category$Resource_Category <- Resource_Category$Var1
Resource_Category$Var1 <- NULL
Resource_Category$Freq <- NULL
Projects_1$Resource_Category <- Projects_1$`Project Resource Category`
#Mapping the numeric ID of Resource Category to the Project table
Projects_1 <- left_join(Projects_1, Resource_Category, by = "Resource_Category")
#Removing unwanted tables created due to join in R.
Projects_1$`Project Resource Category` <- NULL
Projects_1$Resource_Category <- NULL
#Makig final table according to the desired column sequence 
Resource_Category_Final <- data.frame(Resource_Category_ID = Resource_Category$Resource_Category_ID, 
                                      Resource_Category = Resource_Category$Resource_Category)
#Saving the final table in local system
write.table(x = Resource_Category_Final, file = "Resource_Category_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Resource_Category)
#Making a different table for Project Status
Project_Status <- data.frame(table(Projects_1$`Project Current Status`))
#Mapping the numeric ID of Project Status to the Project table
Project_Status$Project_Status_ID <- c(1:3)
Project_Status$Project_Status <- Project_Status$Var1
Project_Status$Var1 <- NULL
Project_Status$Freq <- NULL
Projects_1$Project_Status <- Projects_1$`Project Current Status`
#Mapping the numeric ID of Project Status to the Project table
Projects_1 <- left_join(Projects_1, Project_Status, by = "Project_Status")
Projects_1$`Project Current Status` <- NULL
Projects_1$Project_Status <- NULL
#Makig final table according to the desired column sequence 
Project_Status_Final <- data.frame(Project_Status_ID = Project_Status$Project_Status_ID,
                                   Project_Status = Project_Status$Project_Status)
#Saving the final table in local system
write.table(x = Project_Status_Final, file = "Project_Status_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Project_Status)
#Mapping the Project numeric ID to Donations table
Donations_2 <- left_join(Donations_1, Projects_1, by = "Project ID")
Donations_1$Project_ID <- Donations_2$Project_ID
#Removing unwanted Tables.
rm(Donations_2)
Donations_1$`Project ID` <- NULL
Projects_1$`Project ID` <- NULL
#Makig final table according to the desired column sequence 
Projects_Final <- data.frame(Project_ID = Projects_1$Project_ID, 
                             Teacher_ID = Projects_1$Teacher_ID, 
                             School_ID = Projects_1$School_ID, 
                             Posted_Sequence = Projects_1$`Teacher Project Posted Sequence`, 
                             Title = Projects_1$`Project Title`, 
                             Short_Description = Projects_1$`Project Short Description`, 
                             Need_Statement = Projects_1$`Project Need Statement`, 
                             Cost = Projects_1$`Project Cost`, 
                             Type_ID = Projects_1$Project_Type_ID,
                             Grade_Level_Category_ID = Projects_1$Grade_Level_Category_ID,
                             Resource_Category_ID = Projects_1$Resource_Category_ID,
                             Subject_Category_ID_1 = Projects_1$Subject_Category_ID_1, 
                             Subject_Category_ID_2 = Projects_1$Subject_Category_ID_2,
                             Subject_Category_ID_3 = Projects_1$Subject_Category_ID_3,
                             Subject_Sub_Category_ID_1 = Projects_1$Subject_Sub_Category_ID_1, 
                             Subject_Sub_Category_ID_2 = Projects_1$Subject_Sub_Category_ID_2,
                             Subject_Sub_Category_ID_3 = Projects_1$Subject_Sub_Category_ID_3,
                             Status_ID = Projects_1$Project_Status_ID,
                             Posted_Date = Projects_1$`Project Posted Date`, 
                             Expiration_Date = Projects_1$`Project Expiration Date`, 
                             Fully_Funded_Date = Projects_1$`Project Fully Funded Date`)
#Saving the final table in local system
write.table(x = Projects_Final, file = "Projects_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Projects_1)

#Donations ----------------
#Maing numeric ID for Donations table
Donations_1$Donation_ID <- c(1:4687884)
Donations_1$`Donation ID` <- NULL
#Makig final table according to the desired column sequence 
Donations_Final <- data.frame(Donation_ID = Donations_1$Donation_ID,
                              Project_ID = Donations_1$Project_ID,
                              Donor_ID = Donations_1$Donor_ID,
                              Optional_Donation = Donations_1$`Donation Included Optional Donation`,
                              Amount = Donations_1$`Donation Amount`,
                              Cart_Sequence = Donations_1$`Donor Cart Sequence`,
                              Received_Date = Donations_1$`Donation Received Date`)
#Saving the final table in local system
write.table(x = Donations_Final, file = "Donations_Final.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Donations_1)
rm(Donor_ZIP)
rm(Metro_Type_Id)
rm(State_ID)
#Separating rows which have no valid primary key corresponding to the foreign key columns
Projects_Dirty_1 <- Projects_Final[!(Projects_Final$Teacher_ID %in% Teachers_Final$Teacher_ID),]
Projects_Dirty_2 <- Projects_Final[!(Projects_Final$School_ID %in% Schools_Final$School_Id),]
Projects_Dirty <- rbind(Projects_Dirty_1, Projects_Dirty_2)
Projects_Final <- Projects_Final[!(Projects_Final$Project_ID %in% Projects_Dirty$Project_ID),]
#Separating rows which have no valid primary key corresponding to the foreign key columns
Donations_Dirty_1 <- Donations_Final[!(Donations_Final$Project_ID %in% Projects_Final$Project_ID),]
Donations_Dirty_2 <- Donations_Final[!(Donations_Final$Donor_ID %in% Donors_Final$Donor_Id),]
Donations_Dirty <- rbind(Donations_Dirty_1, Donations_Dirty_2)
Donations_Final <- Donations_Final[!(Donations_Final$Donation_ID %in% Donations_Dirty$Donation_ID),]
#Saving the final table in local system
write.table(x = Donations_Dirty,
            file = "Donations_Dirty.csv", sep =',', row.names = FALSE)
write.table(x = Projects_Dirty,
            file = "Projects_Dirty.csv", sep =',', row.names = FALSE)
#Removing unwanted Tables.
rm(Projects_Dirty_1)
rm(Projects_Dirty_2)
rm(Donations_Dirty_1)
rm(Donations_Dirty_2)
