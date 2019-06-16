use core::borrow::Borrow;
use std::ffi::{CStr, CString};
use std::os::raw::c_void;
use std::ptr::{null, null_mut};
use std::str;

use odbc_sys::*;

fn main() {
    unsafe {
        let mut row: i32 = 0;

        // get environment
        let mut env_handle = null_mut();
        matchReturn("env", SQLAllocHandle(SQL_HANDLE_ENV, null_mut(), &mut env_handle).into(), env_handle, SQL_HANDLE_ENV);

        let mut value: *mut c_void = SQL_OV_ODBC3.into();
        matchReturn("setAttr", SQLSetEnvAttr(env_handle as *mut Env, SQL_ATTR_ODBC_VERSION, value, 0), env_handle, SQL_HANDLE_ENV);

        // alloocate a connection handle
        let mut con_handle = null_mut();
        matchReturn("con_handle", SQLAllocHandle(SQL_HANDLE_DBC, env_handle, &mut con_handle).into(), env_handle, SQL_HANDLE_ENV);

        let con_str = "Driver=MariaDB;server=localhost;database=rust;DB=rust;user=rust;password=rust".as_bytes();
        println!("{}", con_str.len());
        // connect
        matchReturn("con_handle connect", SQLDriverConnect(
            con_handle as *mut Dbc,
            null_mut(),
            con_str.as_ptr(),
            con_str.len() as i16,
            null_mut(),
            0,
            null_mut(),
            SQL_DRIVER_NOPROMPT,
        ), con_handle, SQL_HANDLE_DBC);

        // allocate statement handler
        let mut stmt = null_mut();
        matchReturn("stmt", SQLAllocHandle(SQL_HANDLE_STMT, con_handle, &mut stmt), con_handle, SQL_HANDLE_DBC);

        // retrieve list of tables
        let table_str = "TABLE".as_bytes();
        matchReturn("tables", SQLTables(stmt as *mut Stmt,
                                        null_mut(),
                                        0,
                                        null_mut(),
                                        0,
                                        null_mut(),
                                        0,
                                        table_str.as_ptr(),
                                        SQL_NTS), stmt, SQL_HANDLE_STMT);

        // get collumn count
        let mut columns: SQLSMALLINT = 0;
        matchReturn("count columns", SQLNumResultCols(stmt as *mut Stmt, &mut columns), stmt, SQL_HANDLE_STMT);
        println!("columns {}", columns);

        let mut done = false;
        while !done {
            let ret = SQLFetch(stmt as *mut Stmt);
            if ret == SQL_SUCCESS {
                println!("Row {}", row);
                row = row + 1;

                for i in 1..columns {
                    let mut indicator: i64 = 0;
                    let mut buf: [u8; 512] = [32 as u8; 512];
                    let dat: SQLRETURN = SQLGetData(stmt as *mut Stmt,
                                                    i as u16,
                                                    SQL_C_CHAR,
                                                    buf.as_mut_ptr() as *mut c_void,
                                                    buf.len() as i64,
                                                    &mut indicator);
                    if dat == SQL_SUCCESS {
                        println!("yes");
                        if indicator == SQL_NULL_DATA {
                            print!(" Column {} : {}\n", i, "NULL");
                        } else {
                            print!(" Column {} : {}\n", i, str::from_utf8(&buf).unwrap().trim());
                        }
                    }
                }
            } else {
                if row == 0 {
                    println!("found nothing!!!");
                }
                done = true;
            }
        }


        /* free up allocated handles */
        SQLFreeHandle(SQL_HANDLE_STMT, stmt);
        SQLFreeHandle(SQL_HANDLE_DBC, con_handle);
        SQLFreeHandle(SQL_HANDLE_ENV, env_handle);
    }
}

fn matchReturn(name: &str, sql_return: SQLRETURN, handle: SQLHANDLE, handle_type: HandleType) {
    match sql_return {
        SQL_SUCCESS => println!("{} {}", name, " success"),
        SQL_SUCCESS_WITH_INFO => {
            println!("{} {}", name, " success with info");
            extract_error(handle, handle_type);
        }
        SQL_INVALID_HANDLE => println!("{} {}", name, " invalid handle"),
        SQL_ERROR => {
            println!("{} {}", name, " error");
            extract_error(handle, handle_type);
        }
        other => panic!("{} {}", name, " unexpected SQLRETURN")
    };
}

fn extract_error(handle: SQLHANDLE, handle_type: HandleType) {
    unsafe {
        let mut text_length = 0;
        let mut state = [0; 6];
        let mut native_error = 0;
        let mut rec_number = 1;
        let mut message_text: [u8; 256] = [0 as u8; 256];

        let ret = SQLGetDiagRec(
            handle_type,
            handle,
            rec_number,
            state.as_mut_ptr(),
            &mut native_error,
            message_text.as_mut_ptr(),
            256,
            &mut text_length,
        );
        println!("{} {} {}, {}, {}, {}, {}, {}, {}", text_length, native_error, str::from_utf8(&message_text).unwrap(), state[0], state[1], state[2], state[3], state[4], state[5]);
    }
}