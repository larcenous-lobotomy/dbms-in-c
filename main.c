#include<stdio.h>
#include<stdlib.h>
#include<string.h>

#include "pds.h"
#include "contact.h"

//#define TREPORT(a1,a2) printf("Status %s: %s\n\n",a1,a2); fflush(stdout);

void process_line( char *test_case );
void process_command( char command );

int main()
{
	printf("Program is running...\n");
	char command;
	while(1){
		printf("MENU\n-------------------------------------------\n-------------------------------------------\n");
		printf("Press <o> to open database.\n");
		printf("Press <n> to create new database.\n");
		printf("Press <i> to insert record into database.\n");
		printf("Press <m> to modify record in database.\n");
		printf("Press <d> to delete record in database.\n");
		printf("Press <f> to fetch record based on key.\n");
		printf("Press <x> to exit/close database.\n");
		printf("Press <q> to quit program.\n-------------------------------------------\n-------------------------------------------\n");						
		scanf("%c", &command);
		// printf("line:%s",test_case);
		//if( !strcmp(command,"\n") || !strcmp(command,"") ) continue;
		
		process_command( command );
		printf("\n\n");
		int c;
		while ((c = getchar()) != '\n' && c != EOF) { }		
	}
}

void process_command( char command) 
{
	char test_case[300];
	
	switch(command)
	{
		case 'o': {
			printf("\nTo open database, enter valid filename (without extension):");
			char filename[24];
			scanf("%s", filename);
			strcpy(test_case, "OPEN ");
			strcat(test_case, filename);
			process_line(test_case);
			break;
			}
			
		case 'n': {
			printf("\nTo create database, enter valid filename (without extension):");
			char filename[24];
			scanf("%s", filename);
			strcpy(test_case, "CREATE ");
			strcat(test_case, filename);
			process_line(test_case);			
			break;
			}

		case 'i': {
			printf("To insert record, enter contact details at prompt:\n");
			strcpy(test_case, "STORE ");
			char param[24];
			printf("\nEnter contact-id:");
			scanf("%s", param);
			strcat(test_case, param);
			strcat(test_case, " ");
			printf("\nEnter contact-name:");
			scanf("%s", param);
			strcat(test_case, param);
			strcat(test_case, " ");			
			printf("\nEnter contact-phone-number:");
			scanf("%s", param);
			strcat(test_case, param);	
			process_line(test_case);					
			break;
			}

		case 'm': {
			char param[24];
			char param1[24];		
			printf("To modify record, enter contact details at prompt:\n");
			strcpy(test_case, "MODIFY ");
			printf("\nEnter contact-id:");
			scanf("%s", param);
			strcat(test_case, param);
			strcat(test_case, " ");
			printf("\nEnter changed contact-name:");
			scanf("%s", param);
			printf("\nEnter changed contact-phone-number:");
			scanf("%s", param1);
			strcat(test_case, param);
			strcat(test_case, " ");			
			strcat(test_case, param1);	
			process_line(test_case);	
			break;
			}

		case 'd': {
			char param[24];
			strcpy(test_case, "NDX_DELETE ");
			printf("Enter contact-id:");
			scanf("%s", param);
			strcat(test_case, param);
			process_line(test_case);			
			break;
			}

		case 'f': {
			char param[24];
			printf("Choose type of retrieval key (<N> for NDX key, <n> for Non-NDX key):");
			scanf("%s", param);
			if (strcmp(param, "N") == 0) strcpy(test_case, "NDX_SEARCH ");
			else if (strcmp(param, "n") == 0) strcpy(test_case, "NON_NDX_SEARCH ");
			else {
				printf("No such command is currently defined. Please try again.");
				return;
				}			
			printf("To fetch record, enter retrieval key:");
			scanf("%s", param);
			strcat(test_case, param);
			process_line(test_case);
			break;
			}

		case 'x': {
			printf("Closing database...");
			strcpy(test_case, "CLOSE");
			process_line(test_case);
			break;
			}

		case 'q': {
			printf("Closing database...");
			strcpy(test_case, "CLOSE");
			process_line(test_case);		
			printf("Closing program...\n");
			exit(0);
			break;
			}
			
		default: {
			printf("No such command is currently defined. Please try again.");
			break;
			}			
	}
	
}

void process_line( char *test_case )
{
	char repo_name[30];
	char command[48], param1[48], param2[48], info[1000];
	char phone_num[10];
	int contact_id, status, rec_size, actual_io;
	struct Contact testContact;
	
	//strcpy(testContact.contact_name, "dummy name");
	//strcpy(testContact.phone, "dummy number");
	// strcpy(expected_name, "dummy name");
	// strcpy(expected_phone, "dummy number");
	

	rec_size = sizeof(struct Contact);

	sscanf(test_case, "%s", command);
		
	//printf("Test case: %s", test_case); fflush(stdout);
	if( !strcmp(command,"CREATE") ){
		sscanf(test_case, "%*s %s", repo_name);
		
		status = pds_create( repo_name );

	}

	else if( !strcmp(command,"OPEN") ){
		sscanf(test_case, "%*s %s", repo_name);

		status = pds_open( repo_name, rec_size );
		
		if(status == PDS_REPO_ALREADY_OPEN) printf("Program already has a database open!\n");

	}
	else if( !strcmp(command,"CLOSE") ){

		status = pds_close();

	}	
	else if( !strcmp(command,"STORE") ){

		sscanf(test_case, "%*s %d %s %s", &contact_id, param1, param2);
		testContact.contact_id = contact_id;
		sprintf(testContact.contact_name,"%s",param1);
		sprintf(testContact.phone,"%s",param2);
		status = pds_reload();		
		status = add_contact( &testContact );
		
		if(status == CONTACT_FAILURE) printf("Failed to add record to database.\n");
	}
	else if( !strcmp(command,"MODIFY") ){

		sscanf(test_case, "%*s %d %s %s", &contact_id, param1, param2);
		testContact.contact_id = contact_id;

		sprintf(testContact.contact_name,"%s",param1);
		sprintf(testContact.phone,"%s",param2);

		status = change_contact( contact_id, &testContact );
		if(status == PDS_REC_NOT_FOUND) {
			printf("The specified key does not exist within the database.\n");
			return;
			}
	}	
	else if( !strcmp(command,"NDX_SEARCH") ){

		sscanf(test_case, "%*s %d", &contact_id);
		testContact.contact_id = -1;
		status = search_contact( contact_id, &testContact );
		if(status == PDS_REC_NOT_FOUND) {printf("The specified key does not exist within the database.\n"); return;}
		print_contact(&testContact);
	}
	else if( !strcmp(command,"NON_NDX_SEARCH") ){

		sscanf(test_case, "%*s %s", phone_num);
		testContact.contact_id = -1;
		int actual_io = 0;
		status = search_contact_by_phone( phone_num, &testContact, &actual_io );
		if(status == PDS_REC_NOT_FOUND) {printf("The specified key does not exist within the database.\n"); return;}
		print_contact(&testContact);
	}
	else if( !strcmp(command,"NDX_DELETE") ){

		sscanf(test_case, "%*s %d", &contact_id);
		testContact.contact_id = -1;
		status = delete_contact( contact_id );
		if(status == CONTACT_FAILURE) printf("The specified key does not exist within the database.\n");
	}
}


