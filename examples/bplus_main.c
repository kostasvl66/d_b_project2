#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include "bf.h"
#include "bplus_file_funcs.h"
#include "record_generator.h"



// Macro to handle BF library errors
#define CALL_OR_DIE(call)     \
{                             \
  BF_ErrorCode code = call;   \
  if (code != BF_OK) {        \
    BF_PrintError(code);      \
    exit(code);               \
  }                           \
}

#define OUTPUT = "../output.csv"
// Forward declarations
void insert_records(TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name,int rec_num);

float search_records(TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name,int rec_num);

int get_num(int argc, char *argv[]);

void write_grade(float answer);
void write_test_info(char* team, int rec_num);

int main(int argc, char *argv[]) {
  int rec_num = get_num(argc,argv);  // Default value
  char* team = "default";
  if (argc > 2) {
    team = argv[2];
  }

  BF_Init(LRU);
  // ===== Employee test =====
  write_test_info(team,rec_num);
  const TableSchema employee_schema = employee_get_schema();
  insert_records(employee_schema, employee_random_record, "employees.db",rec_num);
  const float answer = search_records(employee_schema, employee_random_record, "employees.db",rec_num);
  BF_Close();
  write_grade(answer);
}

void write_test_info(char* team,const int rec_num) {
  // write to output file
  FILE *file = fopen("../output.csv", "a");
  if (file == NULL) {
    printf("Error: Could not create or open file!\n");

  }
  fprintf(file, "\n%s,%d,",team, rec_num);
  fclose(file);
}

void write_grade(const float answer) {
  // write to output file
  FILE *file = fopen("../output.csv", "a");
  if (file == NULL) {
    printf("Error: Could not create or open file!\n");

  }
  fprintf(file, "%.1f", answer);
  fclose(file);
}

int get_num(int argc, char *argv[]) {
  int rec_num = 100;  // Default value

  if (argc > 1) {
    // Use strtol for better error checking
    char *endptr;
    errno = 0;  // Reset errno
    long val = strtol(argv[1], &endptr, 10);

    // Check for conversion errors
    if (errno == ERANGE || val > INT_MAX || val < INT_MIN) {
      fprintf(stderr, "Error: Number out of range\n");
      return rec_num;
    }
    if (endptr == argv[1] || *endptr != '\0') {
      fprintf(stderr, "Error: Invalid number format\n");
      return rec_num;
    }
    if (val <= 0) {
      fprintf(stderr, "Error: Record number must be positive\n");
      return rec_num;
    }

    rec_num = (int)val;
    return rec_num;
  }
  return rec_num;
}
/**
 * Inserts random records into a B+ tree file.
 */
void insert_records(const TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name, int rec_num)
{
  // Create and open B+ tree file
  bplus_create_file(&schema, file_name);

  int file_desc;
  BPlusMeta* info;
  bplus_open_file(file_name, &file_desc, &info);

  Record record;
  srand(42); // Deterministic random sequence for reproducibility

  // Insert random records
  for (int i = 0; i < rec_num; i++) {
    random_record(&schema, &record);

    // printf("Insert value: %d\n", record_get_key(&schema, &record));
    // record_print(&schema, &record);

    bplus_record_insert(file_desc, info, &record);
  }
  // Clean up
  bplus_close_file(file_desc, info);

}

/**
 * Searches for random records in the B+ tree file (should find most of them).
 */
float search_records(const TableSchema schema,
                    void (*random_record)(const TableSchema *schema, Record *record),
                    char* file_name,int rec_num)
{
  printf("*************** SEARCHING FOR RECORDS ***************\n");;
  srand(42); // Same seed so the same random keys are searched
  int correct_counter = 0;




  int file_desc;
  BPlusMeta* info;
  bplus_open_file(file_name, &file_desc, &info);


  Record result_value;
  Record record;
  for (int i = 0; i < rec_num; i++) {
    // Creates Random Record
    random_record(&schema, &record);
    // Creates pointer for the result
    Record* result = &result_value;

    const int key = record_get_key(&schema, &record);
    bplus_record_find(file_desc, info, key, &result);

    if (result != NULL) {
      record_print(&schema, result);
      ++correct_counter;
    } else {
      printf("No such record\n");
    }
  }
  bplus_close_file(file_desc, info);
  printf("Percentage: %.1f%%\n",((float)correct_counter*100)/((float)rec_num));
  return ((float)correct_counter*100)/((float)rec_num);
}
