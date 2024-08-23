#include "taos.h"

#if defined (WIN32) | defined (WIN64)
	HMODULE init_taos_handle(std::string version) {
#else
	void* init_taos_handle(std::string version) {
#endif

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif
 
	// 加载动态库
#if defined (WIN32) | defined (WIN64)
    if (version.compare("2.6") == 0){  // 在Windows中使用加载2.6版本动态库
	    handle =  LoadLibrary("taos.2.6.0.34.dll");  
    }else if (version.compare("3.0") == 0){ // 在Windows中使用加载3.0版本动态库
	    handle =  LoadLibrary("taos.3.0.7.1.dll");  
    }else {
        return nullptr;
    }
#else
    if (version.compare("2.6") == 0){  // 加载2.6版本动态库
	    handle = dlopen("./libtaos.2.6.0.34.so", RTLD_LAZY);  // 在Linux/Unix中使用
    }else if (version.compare("3.0") == 0){ // 加载3.0版本动态库
	    handle = dlopen("./libtaos.3.0.7.0.so", RTLD_LAZY);  // 在Linux/Unix中使用
    }else {
        return nullptr;
    }
#endif
    
    return handle;

}

int taos_init(std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif
    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return 1;
    }
    void (*taos_init_default)();  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_init_default = (void (*)())GetProcAddress(handle, "taos_init");  // 在Windows中使用
#else
    taos_init_default = (void (*)())dlsym(handle, "taos_init");  // 在Linux/Unix中使用
#endif

 if (taos_init_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_init: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_init: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return 1;
    }
    // 初始化
    taos_init_default();
    return 0;
}

void taos_cleanup(std::string version){
#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return ;
    }
    void (*taos_cleanup_default)();  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_cleanup_default = (void (*)())GetProcAddress(handle, "taos_cleanup");  // 在Windows中使用
#else
    taos_cleanup_default = (void (*)())dlsym(handle, "taos_cleanup");  // 在Linux/Unix中使用
#endif

 if (taos_cleanup_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_cleanup: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_cleanup: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return ;
    }
    // 清除所有的taos资源
    taos_cleanup_default();
}


TAOS * taos_connect(const char *ip, const char *user, const char *pass, const char *db, uint16_t port,std::string version){
#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return nullptr;
    }
    TAOS * (*taos_connect_default)(const char *ip, const char *user, const char *pass, const char *db, uint16_t port);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_connect_default = (void * (*)(const char *ip, const char *user, const char *pass, const char *db, uint16_t port))GetProcAddress(handle, "taos_connect");  // 在Windows中使用
#else
    taos_connect_default = (void * (*)(const char *ip, const char *user, const char *pass, const char *db, uint16_t port))dlsym(handle, "taos_connect");  // 在Linux/Unix中使用
#endif

 if (taos_connect_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_connect: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_connect: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return nullptr;
    }
    // 获取connect连接的 Taos*
    return taos_connect_default(ip,user,pass,db,port);
}



TAOS_RES * taos_query(TAOS *taos, const char *sqlstr,std::string version){
#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return nullptr;
    }
    TAOS_RES * (*taos_query_default)(TAOS *taos, const char *sqlstr);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_query_default = (TAOS_RES * (*)(TAOS *taos, const char *sqlstr))GetProcAddress(handle, "taos_query");  // 在Windows中使用
#else
    taos_query_default = (TAOS_RES * (*)(TAOS *taos, const char *sqlstr))dlsym(handle, "taos_query");  // 在Linux/Unix中使用
#endif

 if (taos_query_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_query: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_query: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return nullptr;
    }
    // 执行sql
    return taos_query_default(taos,sqlstr);
}


int taos_errno(TAOS_RES *tres,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return 1;
    }
    int (*taos_errno_default)(TAOS_RES *tres);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_errno_default = (int (*)(TAOS_RES *tres))GetProcAddress(handle, "taos_errno");  // 在Windows中使用
#else
    taos_errno_default = (int (*)(TAOS_RES *tres))dlsym(handle, "taos_errno");  // 在Linux/Unix中使用
#endif

 if (taos_errno_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_errno: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_errno: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return 1;
    }
    return taos_errno_default(tres);
}

char * taos_errstr(TAOS_RES *tres,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return nullptr;
    }
    char * (*taos_errstr_default)(TAOS_RES *tres);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_errstr_default = (char * (*)(TAOS_RES *tres))GetProcAddress(handle, "taos_errstr");  // 在Windows中使用
#else
    taos_errstr_default = (char * (*)(TAOS_RES *tres))dlsym(handle, "taos_errstr");  // 在Linux/Unix中使用
#endif

 if (taos_errstr_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_errstr: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_errstr: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return nullptr;
    }
    return taos_errstr_default(tres);
}

void taos_free_result(TAOS_RES *tres,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return ;
    }
    void (*taos_free_result_default)(TAOS_RES *tres);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_free_result_default = (void (*)(TAOS_RES *tres))GetProcAddress(handle, "taos_free_result");  // 在Windows中使用
#else
    taos_free_result_default = (void (*)(TAOS_RES *tres))dlsym(handle, "taos_free_result");  // 在Linux/Unix中使用
#endif

 if (taos_free_result_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_free_result: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_free_result: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return ;
    }
    return taos_free_result_default(tres);
}

void taos_close(TAOS *taos,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return ;
    }
    void (*taos_close_default)(TAOS *taos);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_close_default = (void (*)(TAOS *taos))GetProcAddress(handle, "taos_close");  // 在Windows中使用
#else
    taos_close_default = (void (*)(TAOS *taos))dlsym(handle, "taos_close");  // 在Linux/Unix中使用
#endif

 if (taos_close_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_close: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_close: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return ;
    }
    return taos_close_default(taos);
}


int taos_field_count(TAOS_RES *tres,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return 1;
    }
    int (*taos_field_count_default)(TAOS_RES *tres);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_field_count_default = (int (*)(TAOS_RES *tres))GetProcAddress(handle, "taos_field_count");  // 在Windows中使用
#else
    taos_field_count_default = (int (*)(TAOS_RES *tres))dlsym(handle, "taos_field_count");  // 在Linux/Unix中使用
#endif

 if (taos_field_count_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_field_count: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_field_count: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return 1;
    }
    return taos_field_count_default(tres);
}

TAOS_FIELD *taos_fetch_fields(TAOS_RES *tres,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return nullptr;
    }
    TAOS_FIELD * (*taos_fetch_fields_default)(TAOS_RES *tres);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_fetch_fields_default = (TAOS_FIELD *(*)(TAOS_RES *tres))GetProcAddress(handle, "taos_fetch_fields");  // 在Windows中使用
#else
    taos_fetch_fields_default = (TAOS_FIELD *(*)(TAOS_RES *tres))dlsym(handle, "taos_fetch_fields");  // 在Linux/Unix中使用
#endif

 if (taos_fetch_fields_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_fetch_fields: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_fetch_fields: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return nullptr;
    }
    return taos_fetch_fields_default(tres);
}

TAOS_ROW taos_fetch_row(TAOS_RES *tres,std::string version){

#if defined (WIN32) | defined (WIN64)
	HMODULE handle = nullptr; // 动态库句柄
#else
	void* handle = nullptr;  // 动态库句柄
#endif

    handle = init_taos_handle(version);
    if (!handle) {
#if defined (WIN32) | defined (WIN64)
	    std::cerr << "无法加载动态库: " << GetLastError() << std::endl;  // 在Windows中使用
#else
        std::cerr << "无法加载动态库: " << dlerror() << std::endl;
#endif
        return nullptr;
    }
    TAOS_ROW  (*taos_fetch_row_default)(TAOS_RES *tres);  
    // 获取函数地址
#if defined (WIN32) | defined (WIN64)
	taos_fetch_row_default = (TAOS_ROW (*)(TAOS_RES *tres))GetProcAddress(handle, "taos_fetch_row");  // 在Windows中使用
#else
    taos_fetch_row_default = (TAOS_ROW (*)(TAOS_RES *tres))dlsym(handle, "taos_fetch_row");  // 在Linux/Unix中使用
#endif

 if (taos_fetch_row_default == nullptr) {
#if defined (WIN32) | defined (WIN64)
		std::cerr << "无法找到函数 taos_fetch_row: " << GetLastError() << std::endl;  // 在Windows中使用
		FreeLibrary(handle);  // 在Windows中使用
#else
        std::cerr << "无法找到函数 taos_fetch_row: " << dlerror() << std::endl;
        dlclose(handle);  // 在Linux/Unix中使用
#endif
        return nullptr;
    }
    return taos_fetch_row_default(tres);
}
