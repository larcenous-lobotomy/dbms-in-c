#include<stdio.h>
#include<stdlib.h>
#include<errno.h>
#include<string.h>

#include "pds.h"
#include "bst.h"

struct PDS_RepoInfo repo_handle;


int pds_create(char *repo_name) 
{
  char filename[30], indexfile[30];
  strcpy(filename,repo_name);
  strcpy(indexfile,repo_name);
  strcat(filename,".dat");
  strcat(indexfile,".ndx");
  FILE *fp = fopen(filename,"wb+");
  FILE *ifp = fopen(indexfile,"wb+");
  if(fp  == NULL || ifp == NULL) return PDS_FILE_ERROR;\
  fclose(fp);
  fclose(ifp);
  
  return PDS_SUCCESS;
}


int pds_open(char* repo_name, int rec_size) // Same as before
{
// Open the data file and index file in rb+ mode
// Update the fields of PDS_RepoInfo appropriately
// Build BST and store in pds_bst by reading index entries from the index file
// Close only the index file
	char filename[30], indexfile[30];
	strcpy(filename,repo_name);
	strcpy(indexfile,repo_name);
	strcat(filename,".dat");
	strcat(indexfile,".ndx");
	repo_handle.pds_data_fp = fopen(filename, "rb+");
	repo_handle.pds_ndx_fp = fopen(indexfile, "rb+"); 
	
	if (repo_handle.pds_data_fp == NULL) {
		return PDS_FILE_ERROR;
		}
	if (repo_handle.repo_status == PDS_REPO_OPEN) {
		return PDS_REPO_ALREADY_OPEN;
		}
	if (repo_handle.pds_ndx_fp == NULL) {
		return PDS_LOAD_NDX_FAILED;
		}
	strcpy(repo_handle.pds_name, repo_name);

	repo_handle.repo_status = PDS_REPO_OPEN;

	repo_handle.rec_size = rec_size;

	pds_load_ndx();

	fclose(repo_handle.pds_ndx_fp);

	return PDS_SUCCESS; 
}

int pds_load_ndx() // Same as before
{
// Internal function used by pds_open to read index entries into BST
	fseek(repo_handle.pds_ndx_fp, 0, SEEK_END);
	int size = ftell(repo_handle.pds_ndx_fp);
	int number = size / sizeof(struct PDS_NdxInfo);
	
	if(size > 0) {
		fseek(repo_handle.pds_ndx_fp, 0, SEEK_SET);
		struct PDS_NdxInfo *entries[number];
		for(int i = 0; i < number; i++) {
			entries[i] = (struct PDS_NdxInfo *)malloc(sizeof(struct PDS_NdxInfo));
			fread(entries[i], sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp);
			bst_add_node(&repo_handle.pds_bst, entries[i]->key, entries[i]);	
		}
	}
	
	return 0;
}

int put_rec_by_key(int key, void*rec)
{
  // Seek to the end of the data file
  // Create an index entry with the current data file location using ftell
  // (NEW) ENSURE is_deleted is set to 0 when creating index entry
  // Add index entry to BST using offset returned by ftell
  // Write the key at the current data file location
  // Write the record after writing the key
	if (repo_handle.repo_status == PDS_REPO_OPEN) {
		fseek(repo_handle.pds_data_fp, 0, SEEK_END);
		struct PDS_NdxInfo *entry = (struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));
		entry->key = key;
		entry->offset = ftell(repo_handle.pds_data_fp);
		entry->is_deleted = 0;
		int status = bst_add_node(&repo_handle.pds_bst, key, entry); 
		
		if (status == BST_SUCCESS){
			fwrite(&key, sizeof(int), 1, repo_handle.pds_data_fp);
			fwrite(rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);	
			return PDS_SUCCESS;	
		}
		else if (status == BST_DUP_KEY) {
			free(entry);
			return PDS_ADD_FAILED;
		}
		}
	else {
		return PDS_ADD_FAILED;
	}  
}

int get_rec_by_ndx_key(int key, void*rec)
{
  // Search for index entry in BST
  // (NEW) Check if the entry is deleted and if it is deleted, return PDS_REC_NOT_FOUND
  // Seek to the file location based on offset in index entry
  // Read the key at the current file location 
  // Read the record after reading the key
	struct BST_Node *node = bst_search(repo_handle.pds_bst, key);
	if (node == NULL) {
		return PDS_REC_NOT_FOUND;
		}
	struct PDS_NdxInfo *data_store = (struct PDS_NdxInfo *)(node->data); 
	if (data_store->is_deleted == 1) {
		return PDS_REC_NOT_FOUND;
	}	
	fseek(repo_handle.pds_data_fp, ((struct PDS_NdxInfo *)(node->data))->offset, SEEK_SET);
	int temp_key;
	fread(&temp_key, sizeof(int), 1, repo_handle.pds_data_fp);
	fread(rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);
	return PDS_SUCCESS;  
}

int change_rec_by_ndx_key(int key, void*rec)
{
  // Search for index entry in BST
  // (NEW) Check if the entry is deleted and if it is deleted, return PDS_REC_NOT_FOUND
  // Seek to the file location based on offset in index entry
  // Read the key at the current file location 
  // Read the record after reading the key
	struct BST_Node *node = bst_search(repo_handle.pds_bst, key);
	if (node == NULL) {
		return PDS_REC_NOT_FOUND;
		}
	struct PDS_NdxInfo *data_store = (struct PDS_NdxInfo *)(node->data); 
	if (data_store->is_deleted == 1) {
		return PDS_REC_NOT_FOUND;
	}	
	fseek(repo_handle.pds_data_fp, ((struct PDS_NdxInfo *)(node->data))->offset, SEEK_SET);
	fwrite(&key, sizeof(int), 1, repo_handle.pds_data_fp);
	fwrite(rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);	
	return PDS_SUCCESS;  
}


//Declare local function
void pre_order(struct BST_Node *);

int pds_close() 
{
// Open the index file in wb mode (write mode, not append mode)
// Unload the BST into the index file by traversing it in PRE-ORDER (overwrite the entire index file)
// (NEW) Ignore the index entries that have already been deleted. 
// Free the BST by calling bst_destroy()
// Close the index file and data file
	char filename[30], indexfile[30];
	strcpy(indexfile,repo_handle.pds_name);
	strcat(indexfile,".ndx");
	
	repo_handle.pds_ndx_fp = fopen(indexfile, "wb+");

	if (repo_handle.pds_ndx_fp == NULL) {
		return PDS_LOAD_NDX_FAILED;
		}
		
	// Preorder Traversal
	pre_order(repo_handle.pds_bst);
	
	fclose(repo_handle.pds_ndx_fp);		
	
	bst_destroy(repo_handle.pds_bst);
	fclose(repo_handle.pds_data_fp);
	repo_handle.repo_status = PDS_REPO_CLOSED;

	return PDS_SUCCESS;
}

int pds_reload() 
{
// Open the index file in wb mode (write mode, not append mode)
// Unload the BST into the index file by traversing it in PRE-ORDER (overwrite the entire index file)
// (NEW) Ignore the index entries that have already been deleted. 
// Free the BST by calling bst_destroy()
// Close the index file and data file
	char filename[30], indexfile[30];
	strcpy(filename,repo_handle.pds_name);	
	int rec_size = repo_handle.rec_size;	
	
	int status = pds_close();
	
	status = pds_open(filename, rec_size);
}

int get_rec_by_non_ndx_key(void *key, void *rec, int (*matcher)(void *rec, void *key), int *io_count)
{
  // Seek to beginning of file
  // Perform a table scan - iterate over all the records
  //   Read the key and the record
  //   Increment io_count by 1 to reflect count no. of records read
  //   Use the function in function pointer to compare the record with required key
  //   (NEW) Check the entry of the record in the BST and see if it is deleted. If so, return PDS_REC_NOT_FOUND
  // Return success when record is found
  fseek(repo_handle.pds_data_fp, 0, SEEK_SET);
  int temp_key;
  int compare;
	while(fread(&temp_key, sizeof(int), 1, repo_handle.pds_data_fp) != 0) {
		fread(rec, repo_handle.rec_size, 1, repo_handle.pds_data_fp);
		(*io_count)++;
		compare = (*matcher)(rec, key);
		if (compare == 0) {
			struct BST_Node *node = bst_search(repo_handle.pds_bst, temp_key);
			if (node == NULL) {
				break;
				}
			struct PDS_NdxInfo *data_store = (struct PDS_NdxInfo *)(node->data); 	
			if (data_store->is_deleted == 1) {
				break;
			}	
			return PDS_SUCCESS;
		}
	}  
  return PDS_REC_NOT_FOUND;  
}

int delete_rec_by_ndx_key( int key) // New Function
{
  // Search for the record in the BST using the key
  // If record not found, return PDS_DELETE_FAILED
  // If record is found, check if it has already been deleted, if so return PDS_DELETE_FAILED
  // Else, set the record to deleted and return PDS_SUCCESS
	struct BST_Node *node = bst_search(repo_handle.pds_bst, key);
	if (node == NULL) {
		return PDS_DELETE_FAILED;
		}  
	struct PDS_NdxInfo *data_store = (struct PDS_NdxInfo *)(node->data);
	if (data_store->is_deleted == 1) {
		return PDS_DELETE_FAILED;
	}	
	data_store->is_deleted = 1;
	return PDS_SUCCESS;
}

//Local function for iterative preorder traversal
void pre_order(struct BST_Node *root) {
	if (root != NULL){
		struct PDS_NdxInfo *data_store = (struct PDS_NdxInfo *)(root->data); 
		if (data_store->is_deleted != 1) {
			struct PDS_NdxInfo *entry = (struct PDS_NdxInfo *) malloc(sizeof(struct PDS_NdxInfo));
			entry->key = root->key;
			entry->offset = ((struct PDS_NdxInfo *)(root->data))->offset;
			fwrite(entry, sizeof(struct PDS_NdxInfo), 1, repo_handle.pds_ndx_fp);
		}
	}
	else if (root == NULL) {
		return;
	}

	pre_order(root->left_child);
	pre_order(root->right_child);
}
