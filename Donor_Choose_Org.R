
## app.R ##

#library----
library(shiny)
library(DBI)
library(pool)
library(shinydashboard)
library(RMariaDB)
library(RMySQL)  
library(ggplot2)
library(dplyr)
library(DT) 
library(plotly)
library(data.table)
library(readr) # to read_csv function
library(data.table)
library(base)
library(leaflet)
library(lubridate)
library(gridExtra)
library(scales)
library(pool)
library("tidyverse")
library("dplyr")
library("tidyr")
library(class)

#Create connection from R shiny app to the MariaDB (database: Project) hosted at the AWS server with correct user credentials 
conn <- DBI::dbConnect(
  drv = RMariaDB::MariaDB(),
  host = "ec2-3-19-242-132.us-east-2.compute.amazonaws.com", 
  db= "Project_Final",
  username = "Deeksha",
  password = "1234"
)


#First Tab: Queries to fetch data from database -----
Donations <- dbGetQuery(conn, 'Select * from Donations;')
names(Donations) <- make.names(names(Donations))

Donors <- dbGetQuery(conn, 'Select * from Donors;')
names(Donors)<-make.names(names(Donors))

Schools <- dbGetQuery(conn, 'Select * from Schools;')
names(Schools) <- make.names(names(Schools))

Teachers <- dbGetQuery(conn, 'Select * from Teachers;')
names(Teachers)<-make.names(names(Teachers))

Projects <- dbGetQuery(conn, 'Select * from Projects;')
names(Projects)<-make.names(names(Projects))



#Second Tab: Queries to fetch data from database -----
query_project_category<- "select * from Project_Type;"
Project_Type <- dbGetQuery(conn, query_project_category)

query_school_location <- "select District from School;"
District <- dbGetQuery(conn, query_school_location)

query_grade_level <- "select * from Grade_Level;"
Grade_Level <- dbGetQuery(conn, query_grade_level)

query_metro_type <- "select * from Metro_Type;"
Metro_Type <- dbGetQuery(conn, query_metro_type)

query_project_title<- "select Title from Project Order By 'Project ID' DESC LIMIT 20;"
Project_Title <- dbGetQuery(conn, query_project_title)


#Queries to fetch data for the prediction model
query_pred_model_data <- "SELECT P.Posted_Date, P.Fully_Funded_Date, P.Cost, P.Project_Type_ID, S.District, 
                        P.Grade_Level_ID, S.Metro_Type_ID FROM Project P
                        JOIN School S ON P.School_ID = S.School_ID 
                        JOIN Project_Type PT ON PT.Project_Type_ID = P.Project_Type_ID
                        JOIN Grade_Level GL ON P.Grade_Level_ID = GL.Grade_Level_ID
                        JOIN Metro_Type MT ON S.Metro_Type_ID = MT.Metro_Type_ID
                        WHERE Project_Status_ID = 2;"
pred_model_data <- dbGetQuery(conn, query_pred_model_data)
pred_model_data <- na.omit(pred_model_data)
pred_model_data$DaysTaken <- (as.Date(pred_model_data$Fully_Funded_Date) - as.Date(pred_model_data$Posted_Date))
pred_model_data$DaysTakenGroup <- (ifelse(pred_model_data$DaysTaken >= 150, 11, 
                                    ifelse(pred_model_data$DaysTaken >= 135,10, 
                                    ifelse(pred_model_data$DaysTaken >= 120,9, 
                                    ifelse(pred_model_data$DaysTaken >= 105,8, 
                                    ifelse(pred_model_data$DaysTaken >= 90, 7, 
                                    ifelse(pred_model_data$DaysTaken >= 75, 6, 
                                    ifelse(pred_model_data$DaysTaken >= 60, 5, 
                                    ifelse(pred_model_data$DaysTaken >= 45, 4, 
                                    ifelse(pred_model_data$DaysTaken >= 30, 3, 
                                    ifelse(pred_model_data$DaysTaken >= 15, 2, 1 )))))))))))
pred_model_data <- pred_model_data %>% separate(Posted_Date, c("Post_Year","Post_Month","Post_Date"), "-")
pred_model_data$Fully_Funded_Date <- NULL
pred_model_data$DaysTaken <- NULL
pred_model_data$Post_Year <- NULL
pred_model_data$Post_Date <- NULL

#Functions to fetch input from the UI into fields required for the prediction---
fields2 <- c("Post_month_1","Cost_1","Project_Type_1","District_1","Grade_level_1","Metro_Type_1")
fields3 <- c("ID3")

#To fetch project category
get_query_data <- function(q){
  return(dbGetQuery(conn, q))
  dbDisconnect(db)
}

on.exit(dbDisconnect(conn))



#Prediction Function function----
pred_model_function <- function(data) {
  responses <<- data
  print("Please wait while the model is trained.")
  print("It may take upto 15 minutes.")
  #Adding 'X' so that we can get the predicted value later. It is cumpulsory to have a value in the column.
  responses$DaysTakenGroup <- sample(1:5,1)
  #Making Column names same to ensure 'rbind' function works.
  names(responses) <- names(pred_model_data)
  pred_model_data <- rbind(pred_model_data, responses)
  #Making Cost Group to transform data for prediction model.
  pred_model_data$CostGroup <- (ifelse(pred_model_data$Cost >= 1500, 6,
                                ifelse(pred_model_data$Cost >= 1200, 5,
                                ifelse(pred_model_data$Cost >= 900, 4,
                                ifelse(pred_model_data$Cost >= 600, 3,
                                ifelse(pred_model_data$Cost >= 300, 2, 1))))))
  
  #Mapping numeric values instead of District character. Making another District table,
  # adding a numeric primary key and then making a join to map the numeric values.
  District_Numeric <- as.data.frame(table(pred_model_data$District))
  District_Numeric$District_ID <- c(1:length(table(pred_model_data$District)))
  pred_model_data <- inner_join(pred_model_data, District_Numeric, by = c("District" = "Var1"))
  pred_model_data <- na.omit(pred_model_data)
  #Deleting the District Numeric table.
  rm(District_Numeric)
  pred_model_data$District <- NULL
  #Making Table for KNN prediction, according to required variables in the correct order.
  Project_Schools_KNN_Table <- data.frame(DaysTakenGroup = pred_model_data$DaysTakenGroup,
                                          Project_Type = as.numeric(pred_model_data$Project_Type_ID),
                                          Project_Cost_Group = as.numeric(pred_model_data$CostGroup),
                                          School_District = as.numeric(pred_model_data$District_ID),
                                          Project_Grade_level_Category = as.numeric(pred_model_data$Grade_Level_ID),
                                          Post_Month = as.numeric(pred_model_data$Post_Month),
                                          School_Metro_Type = as.numeric(pred_model_data$Metro_Type_ID))
  
  #Normilization Function to normalize all the columns.
  normalize <- function(x) { (x - min(x))/(max(x) - min(x)) }
  #Applying the Normalization function to all rows of the table except 1st column - which we want to predict.
  Project_Schools_KNN_Table_Normalised <- as.data.frame(Project_Schools_KNN_Table$DaysTakenGroup)
  Project_Schools_KNN_Table_Normalised <- as.data.frame(lapply((Project_Schools_KNN_Table[,c(1:7)]), normalize))
  #Breaking the table into test and train tables.
  Project_Schools_KNN_Table_Normalised_req <- Project_Schools_KNN_Table_Normalised[length(Project_Schools_KNN_Table$DaysTakenGroup), ]
  Project_Schools_KNN_Table_Normalised <- Project_Schools_KNN_Table_Normalised[-length(Project_Schools_KNN_Table$DaysTakenGroup), ] 
  trainset <- createDataPartition(y = Project_Schools_KNN_Table_Normalised$DaysTakenGroup, p=0.9 , list = F)
  Project_Schools_KNN_Table_Normalised_Train <- Project_Schools_KNN_Table_Normalised[trainset,]
  Project_Schools_KNN_Table_Normalised_Test <- Project_Schools_KNN_Table_Normalised[-trainset,]
  Project_Schools_KNN_Table_Normalised_Test <- rbind(Project_Schools_KNN_Table_Normalised_Test,Project_Schools_KNN_Table_Normalised_req)
  #Project_Schools_KNN_Table_Normalised_Train <- Project_Schools_KNN_Table_Normalised[c(1:744000),]
  #Project_Schools_KNN_Table_Normalised_Test <- Project_Schools_KNN_Table_Normalised[c(744001:length(Project_Schools_KNN_Table_Normalised$DaysTakenGroup)),]
  #Prediting by training the KNN model.
  library(class)
  pred_model <- knn(Project_Schools_KNN_Table_Normalised_Train, Project_Schools_KNN_Table_Normalised_Test, 
                    Project_Schools_KNN_Table[trainset,1], k= 19)
  #Displaying the prediction value - 
  pred_ans <- ifelse(pred_model[length(pred_model)] == 1 , "Less Than 15 Days",
                       ifelse(pred_model[length(pred_model)] == 2 , "Between 15 and 30 days",
                       ifelse(pred_model[length(pred_model)] == 3 , "Between 30 and 45 days",
                       ifelse(pred_model[length(pred_model)] == 4 , "Between 45 and 60 days",
                       ifelse(pred_model[length(pred_model)] == 5 , "Between 60 and 75 days",
                       ifelse(pred_model[length(pred_model)] == 6 , "Between 75 and 90 days",
                       ifelse(pred_model[length(pred_model)] == 7 , "Between 90 and 105 days",
                       ifelse(pred_model[length(pred_model)] == 8 , "Between 105 and 120 days",
                       ifelse(pred_model[length(pred_model)] == 9 , "Between 120 and 135 days",
                       ifelse(pred_model[length(pred_model)] == 10 , "Between 135 and 150 days",
                                                                     "More than 150 days"))))))))))

  comp_table <- table(pred_model,Project_Schools_KNN_Table[-trainset,1])
  print(comp_table)
  sum = 0.0
  #Calculate the accuracy of the model
  for (i in c(1:ncol(comp_table))) {
    sum = sum + as.numeric(comp_table[i,i])
  }
  
  acc = (sum/length(pred_model))*100
  ans <<- data.frame(Predicted_Time = pred_ans ,
                     Accuracy = acc)
  return(ans)
  print(ans)
}

#UI of the application ----
ui <- dashboardPage(
  dashboardHeader(title = "Donor Org Dashboard"),
  ## Sidebar content
  dashboardSidebar(
    sidebarMenu(
      menuItem("Analytics", tabName = "analytics", icon = icon("bar-chart-o")),
      menuItem("Success Predictor", tabName = "predictor", icon = icon("dashboard")),
      menuItem("Get Donors", tabName = "donors", icon = icon("th"))
    )
  ),
  dashboardBody(
    tabItems(
      
      # First tab content----
      tabItem(tabName = "analytics",
              h2("Graphical Insights"),
              mainPanel(splitLayout(style = "height:206px;border: 1px solid black;",
                                    cellWidths = 250,
                                    cellArgs = list(style = "padding: 3px"),
                                    plotOutput("coolplot1", height = "200px"),
                                    plotOutput("coolplot2", height = "200px"),
                                    plotOutput("coolplot3", height = "200px")),
                        br(), br(),
                        splitLayout(style = "height:206px;border: 1px solid black;",
                                    cellWidths = 350,
                                    cellArgs = list(style = "padding: 3px"),
                                    plotOutput("coolplot4", height = "200px"),
                                    plotOutput("coolplot5", height = "200px")),
                        br(), br(),
                        splitLayout(style = "height:206px;border: 1px solid black;",
                                    cellWidths = 250,
                                    cellArgs = list(style = "padding: 3px"),
                                    plotOutput("coolplot6", height = "200px"),
                                    plotOutput("coolplot7", height = "200px"),
                                    plotOutput("coolplot8", height = "200px")),
                        tableOutput("results")           
              )        
      ),
      # Second tab content----
      tabItem(tabName = "predictor",
              h2("Project Information"),
              selectInput("Post_month_1", "Choose your Post Month:", choices = c(1,2,3,4,5,6,7,8,9,10,11,12)),
              textInput("Cost_1", "Enter your Project Cost"),
              selectInput("Project_Type_1", "Choose your Project Type:", choices = Project_Type),
              selectInput("District_1", "Choose your School District", choices = District),
              selectInput("Grade_level_1", "Choose your Project Grade Level Category:", choices = Grade_Level),
              selectInput("Metro_Type_1", "Choose your School Metro Type:", choices = Metro_Type),
              actionButton("submit", "Submit"),
              tableOutput("resText"), 
              tableOutput("res"), tags$hr(),
              tableOutput("resOutput")
      ),
      # Third tab content----
      tabItem(tabName = "donors",
              h2("Get Donors List"),
              selectInput("ID3", "Choose your relevant Project Title:", choices = Project_Title) ,
              actionButton("submit3", "Submit"),
              tableOutput("res3"), tags$hr()
              
      ) 
    )
  )
)



#Server code of application-----
server <- function(input, output, session) {
  #first tab----
  {
    output$countryOutput <- renderUI({
    })  
    filtered <- reactive({
      if (is.null(input$countryInput)) {
        return(NULL)
      }    
    })
    output$coolplot1 <- renderPlot({
      pie(table(donor_city_count$Donor_Is_Teacher),
          labels = paste(z$Var1, z$label),
          main = "Donor is Teacher")
    })
    output$coolplot2 <- renderPlot({
      pie(table(donor_city_count$same_city),
          labels = paste(x$Var1, x$label),
          main = "% of Same City Donors \n Including the teachers.")
    })
    output$coolplot3 <- renderPlot({
      pie(table(donor_city_count$same_city[donor_city_count$Donor_Is_Teacher == "No"]),
          labels = paste(y$Var1, y$label),
          main = "% of Same City Donors \n Excluding the teachers.")
    })
    output$coolplot4 <- renderPlot({
      barplot(table(pred_model_data$Post_Month),las=2, cex.names=.9, ylim = c(0,150000),
              main = "Number of Projects Posted \n in every month",
              xlab = "Month")
    })
    output$coolplot5 <- renderPlot({
      barplot(table(donor_city_count$Received_Month),las=2, cex.names=.9, ylim = c(0,700000),
              main = "Number of Donation made \n in every month",
              xlab = "Month")
    })
    output$coolplot6 <- renderPlot({
      barplot(table(donor_city_count$Post_Year),
              #ylim = c(0,450000),
              main = "Number of Projects posted ",
              xlab = "Year")
    })
    output$coolplot7 <- renderPlot({
      barplot(table(donor_city_count$Received_Year[donor_city_count$Donor_Is_Teacher == "Yes"]),
              ylim = c(0,450000),
              main = "Number of Donation made \n by teachers",
              xlab = "Year")
    })
    output$coolplot8 <- renderPlot({
      barplot(table(donor_city_count$Received_Year[donor_city_count$Donor_Is_Teacher == "No"]),
              #ylim = c(0,450000),
              main = "Number of Donation made \n by other donors",
              xlab = "Year")
    })
  }
  #Second tab ----
  #reactive value to get the logical output
  rv <- reactiveVal()
  #method to generate output on the click of submit button in tab 2
  observeEvent(input$submit, {
    df <- as.data.frame(
      t(sapply(fields2, function(X) input[[X]]))
    ) 
    df2 <- pred_model_function(df)
    rv(rbind(df2))
  })
  # Update with current input responses selected when Submit is clicked
  output$res <- renderTable({
    input$submit
    rv()
  })
  output$resText <- renderText("")
  #Third tab-----
    observeEvent(input$submit3, {
    titleinput <- as.character(input[[fields3]])
    output$res3 <- renderTable({
      dbGetQuery(conn, paste("Select Donor_ID, Is_Teacher from Donor 
                  where City=(Select City From School S JOIN Project P 
                  ON P.School_ID = S.School_ID AND Title = '", titleinput, "' LIMIT 1) LIMIT 15;",sep = ""))
    })  
  })
}
#Shiny app call----
shinyApp(ui, server)
