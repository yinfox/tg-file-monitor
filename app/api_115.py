"""完整的115 API 客户端（从主工程复制）"""
import requests
import json
import time
import os
from typing import Optional, Dict, Any
from urllib.parse import urlencode

# 导入p115client用于文件复制
try:
    from p115client import P115Client
    from p115client.util import share_extract_payload
    P115CLIENT_AVAILABLE = True
except ImportError:
    P115CLIENT_AVAILABLE = False
    share_extract_payload = None
    print("[警告] p115client未安装，文件复制功能不可用")

class Client115:
    """115网盘API客户端"""
    
    def __init__(self, cookie: str = None):
        """
        初始化客户端
        
        Args:
            cookie: 115网盘的Cookie字符串
        """
        self.base_url = 'https://api.115.com'
        self.cookie = cookie
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        if cookie:
            self.session.headers.update({
                'Cookie': cookie
            })
        
        # 初始化p115client用于文件复制
        self.p115_client = None
        if P115CLIENT_AVAILABLE and cookie:
            try:
                self.p115_client = P115Client(cookie, check_for_relogin=False)
                print("[P115Client] 初始化成功，文件复制功能已启用")
            except Exception as e:
                print(f"[P115Client] 初始化失败: {e}")
    
    def set_cookie(self, cookie: str):
        """设置Cookie"""
        self.cookie = cookie
        self.session.headers.update({
            'Cookie': cookie
        })
        
        # 更新p115client的cookie
        if P115CLIENT_AVAILABLE:
            try:
                self.p115_client = P115Client(cookie, check_for_relogin=False)
                print("[P115Client] Cookie已更新")
            except Exception as e:
                print(f"[P115Client] Cookie更新失败: {e}")
    
    def _extract_userid_from_cookie(self) -> Optional[str]:
        """从Cookie中提取用户ID"""
        if not self.cookie:
            return None
        
        # 115的Cookie中通常包含 UID=xxx 的格式
        for item in self.cookie.split(';'):
            item = item.strip()
            if item.startswith('UID='):
                return item.split('=', 1)[1]
            if item.startswith('userId='):
                return item.split('=', 1)[1]
        
        # 如果无法从Cookie中提取，返回None，将使用其他方式
        return None
    
    def check_file_exists(self, file_sha1: str, file_size: int, file_name: str, target: str = 'U_1_0', file_path: str = None) -> Dict[str, Any]:
        """
        检查文件是否秒传可用（真正的秒传逻辑）
        使用115的upload_file API，会自动判断秒传
        
        Args:
            file_sha1: 文件SHA1哈希值
            file_size: 文件大小（字节）
            file_name: 文件名
            target: 目标文件夹ID (默认 U_1_0 = 根目录)
            file_path: 文件路径（用于真正上传）
        
        Returns:
            包含秒传结果的字典
        """
        if not self.cookie:
            return {
                'success': False,
                'message': '未设置Cookie，请先登录',
                'can_transfer': False,
                'error': 'NO_COOKIE'
            }
        
        if not self.p115_client:
            return {
                'success': False,
                'message': 'p115client未初始化',
                'can_transfer': False,
                'error': 'P115CLIENT_NOT_AVAILABLE'
            }
        
        print("\n" + "="*60)
        print("[秒传上传请求]")
        print(f"文件名: {file_name}")
        print(f"文件大小: {file_size} bytes ({file_size/1024/1024:.2f} MB)")
        print(f"SHA1: {file_sha1}")
        print(f"目标文件夹: {target}")
        print("="*60)
        
        try:
            # ======== 步骤1：获取userkey ========
            print("\n[步骤1] 获取上传凭证（userkey）...")
            
            headers = {
                'Cookie': self.cookie,
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'
            }
            
            # 提取target中的CID
            target_cid = target.split('_')[-1] if '_' in target else target
            
            data = {
                'app_ver': '25.2.0',
                'filename': file_name,
                'filesize': file_size,
                'fileid': file_sha1,
                'target': target,
            }
            
            resp = self.session.post(
                'https://proapi.115.com/app/uploadinfo',
                headers=headers,
                data=data,
                timeout=10
            )
            
            upload_info = resp.json()
            
            if not upload_info.get('state'):
                print(f"❌ 获取上传凭证失败: {upload_info}")
                return {
                    'success': False,
                    'message': '获取上传凭证失败',
                    'can_transfer': False,
                    'error': 'GET_USERKEY_FAILED'
                }
            
            userkey = upload_info.get('userkey')
            if not userkey:
                print(f"❌ 响应中没有userkey")
                return {
                    'success': False,
                    'message': '获取userkey失败',
                    'can_transfer': False,
                    'error': 'NO_USERKEY'
                }
            
            print(f"✅ 获取到userkey: {userkey[:20]}...")
            
            # ======== 步骤2：设置userkey并上传（自动秒传）========
            print(f"\n[步骤2] 执行上传（自动判断秒传）...")
            
            # 将userkey设置到p115client
            self.p115_client.__dict__['user_key'] = userkey
            
            # 调用upload_file - 如果文件存在会自动秒传（status=2）
            if not file_path or not os.path.exists(file_path):
                print(f"❌ 文件路径无效: {file_path}")
                return {
                    'success': False,
                    'message': '文件路径无效',
                    'can_transfer': False,
                    'error': 'INVALID_FILE_PATH'
                }
            
            upload_result = self.p115_client.upload_file(
                file_path,
                pid=target_cid,
                filesha1=file_sha1,
                filesize=file_size
            )
            
            print(f"[上传结果] {json.dumps(upload_result, ensure_ascii=False, indent=2)}")
            
            # 检查结果
            upload_status = upload_result.get('status')
            print(f"[DEBUG] upload_status type: {type(upload_status)}, value: {upload_status}")
            
            if upload_status == 2:
                # 秒传成功！
                print(f"🎉 秒传成功！文件已存在无需上传")
                pickcode = upload_result.get('pickcode', '')
                file_id = upload_result.get('data', {}).get('id', '')
                
                return {
                    'success': True,
                    'message': f'✅ 秒传成功！\n文件：{file_name}\n大小：{file_size/1024/1024:.2f}MB',
                    'can_transfer': True,
                    'already_exists': True,
                    'transferred': True,  # 已经传到目标文件夹了
                    'pickcode': pickcode,
                    'file_id': file_id,
                    'response': upload_result
                }
            elif upload_status == 1:
                # 需要真实上传
                print(f"⚠️  文件不存在，需要真实上传")
                return {
                    'success': False,
                    'message': f'文件不存在于115服务器，需要真实上传',
                    'can_transfer': False,
                    'need_upload': True,
                    'response': upload_result
                }
            # 检查是否是实际上传成功（非秒传）
            elif upload_result.get('state') == True and upload_result.get('data', {}).get('file_id'):
                # 实际上传成功了
                print(f"📤 实际上传成功（非秒传）")
                file_id = upload_result['data']['file_id']
                pickcode = upload_result['data'].get('pick_code', '')
                return {
                    'success': True,
                    'message': f'✅ 实际上传成功\n文件：{file_name}\n大小：{file_size/1024/1024:.2f}MB',
                    'can_transfer': True,
                    'already_exists': False,
                    'transferred': True,  # 已经传到目标文件夹了
                    'pickcode': pickcode,
                    'file_id': file_id,
                    'response': upload_result
                }
            else:
                # 其他状态
                status_msg = upload_result.get('statusmsg', '') or upload_result.get('message', '')
                print(f"⚠️  上传状态: {upload_result.get('status')} - {status_msg}")
                return {
                    'success': False,
                    'message': f'上传失败: {status_msg}',
                    'can_transfer': False,
                    'error': status_msg,
                    'response': upload_result
                }
            
        except Exception as e:
            print(f"[异常] {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'message': f'秒传上传出错: {str(e)}',
                'can_transfer': False,
                'error': str(e)
            }
    
    def copy_file_to_folder(self, file_id: str, target_cid: str, file_name: str = "") -> Dict[str, Any]:
        """
        将秒传找到的文件复制到目标文件夹
        
        Args:
            file_id: 通过SHA1搜索找到的文件ID
            target_cid: 目标文件夹的CID
            file_name: 文件名（用于日志）
            
        Returns:
            Dict: 包含操作结果的字典
        """
        print(f"\n{'='*60}")
        print(f"[复制文件到文件夹]")
        print(f"File ID: {file_id}")
        print(f"Target CID: {target_cid}")
        print(f"File Name: {file_name}")
        print(f"{'='*60}")
        
        if not self.p115_client:
            error_msg = "p115client未初始化，无法复制文件"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'message': f'文件复制失败\n原因：{error_msg}',
                'transferred': False,
                'error': 'P115CLIENT_NOT_AVAILABLE'
            }
        
        try:
            print(f"[执行] 调用 p115client.fs_copy({file_id}, {target_cid})")
            
            # 使用p115client的fs_copy方法（不是fs_copy_app）
            result = self.p115_client.fs_copy(file_id, target_cid)
            
            print(f"[复制结果] {result}")
            
            # 检查返回结果
            # p115client.fs_copy返回格式: {'state': True/False, 'error': '...', 'errno': ...}
            if isinstance(result, dict):
                # 检查state字段判断是否成功
                if result.get('state') == True:
                    print(f"✅ 文件复制成功！")
                    
                    # 🆕 复制后重命名文件
                    try:
                        print(f"[执行] 重命名文件为用户指定的名称: {file_name}")
                        
                        # fs_copy后，file_id仍然有效（指向目标文件夹中的新副本）
                        # 调用fs_rename重命名：payload参数需要是tuple (file_id, new_name)
                        rename_result = self.p115_client.fs_rename((file_id, file_name))
                        print(f"[重命名结果] {rename_result}")
                        
                        if rename_result.get('state') == True:
                            print(f"✅ 文件重命名成功：{file_name}")
                        else:
                            print(f"⚠️  文件重命名失败: {rename_result.get('error', '未知错误')}")
                            print(f"   文件已复制但保留原名称")
                            
                    except Exception as rename_error:
                        print(f"⚠️  重命名文件时出错: {rename_error}")
                        print(f"   文件已复制但保留原名称")
                    
                    return {
                        'success': True,
                        'message': f'✅ 秒传+复制成功！\n文件：{file_name}\n已添加到目标文件夹',
                        'transferred': True,
                        'file_id': file_id,
                        'target_cid': target_cid,
                        'copy_result': result
                    }
                else:
                    # 复制失败，返回错误信息
                    error_msg = result.get('error', '未知错误')
                    errno = result.get('errno', '')
                    print(f"❌ 文件复制失败: {error_msg} (errno: {errno})")
                    return {
                        'success': False,
                        'message': f'文件复制失败\n文件：{file_name}\n错误：{error_msg}',
                        'transferred': False,
                        'error': error_msg,
                        'errno': errno,
                        'copy_result': result
                    }
            else:
                print(f"⚠️  复制操作返回非预期格式")
                return {
                    'success': False,
                    'message': f'文件复制失败\n文件：{file_name}\n返回格式错误',
                    'transferred': False,
                    'error': 'UNEXPECTED_FORMAT'
                }
                
        except AttributeError as e:
            error_msg = f"p115client不支持fs_copy_app方法: {e}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'message': f'文件复制失败\n原因：方法不存在',
                'transferred': False,
                'error': 'METHOD_NOT_FOUND'
            }
        except Exception as e:
            error_msg = f"复制文件时出错: {str(e)}"
            print(f"❌ {error_msg}")
            return {
                'success': False,
                'message': f'文件复制失败\n原因：{str(e)}',
                'transferred': False,
                'error': str(e)
            }

    def _extract_share_items(self, snap_resp: Dict[str, Any]) -> list:
        if not isinstance(snap_resp, dict):
            return []
        data = snap_resp.get('data')
        candidates = []
        if isinstance(data, list):
            candidates = data
        elif isinstance(data, dict):
            for key in ('list', 'files', 'items', 'data'):
                value = data.get(key)
                if isinstance(value, list):
                    candidates = value
                    break
        elif isinstance(snap_resp.get('list'), list):
            candidates = snap_resp.get('list')

        items = []
        for entry in candidates or []:
            if not isinstance(entry, dict):
                continue
            fid = (
                entry.get('file_id')
                or entry.get('fid')
                or entry.get('id')
                or entry.get('fileid')
                or entry.get('cid')
            )
            name = (
                entry.get('file_name')
                or entry.get('name')
                or entry.get('n')
                or entry.get('fn')
                or entry.get('title')
            )
            is_dir = bool(entry.get('is_dir') or entry.get('is_directory') or entry.get('isfolder'))
            if not is_dir:
                ftype = entry.get('file_type')
                if isinstance(ftype, (int, str)) and str(ftype) in ('1', '2', 'folder', 'dir'):
                    is_dir = True
            size = entry.get('file_size') or entry.get('size') or entry.get('fs') or 0
            if fid:
                items.append({
                    'id': str(fid),
                    'name': str(name or fid),
                    'is_dir': is_dir,
                    'size': size,
                })
        return items

    def receive_share_link(
        self,
        link: str,
        target_cid: str = "",
        file_ids: Optional[list] = None,
        accept_all: bool = False,
        list_limit: int = 200,
    ) -> Dict[str, Any]:
        if not self.p115_client:
            return {
                'success': False,
                'message': 'p115client未初始化，无法转存分享链接',
                'error': 'P115CLIENT_NOT_AVAILABLE'
            }
        if not share_extract_payload:
            return {
                'success': False,
                'message': 'p115client工具不可用，无法解析分享链接',
                'error': 'P115CLIENT_UTIL_MISSING'
            }

        try:
            payload = share_extract_payload(link)
        except Exception as e:
            return {
                'success': False,
                'message': f'分享链接解析失败: {e}',
                'error': 'INVALID_SHARE_LINK'
            }

        share_code = payload.get('share_code')
        receive_code = payload.get('receive_code') or ""
        if not share_code:
            return {
                'success': False,
                'message': '分享链接缺少 share_code',
                'error': 'NO_SHARE_CODE'
            }

        selected_ids = [str(fid) for fid in (file_ids or []) if fid]
        items = []
        if not selected_ids:
            try:
                snap_resp = self.p115_client.share_snap({
                    'share_code': share_code,
                    'receive_code': receive_code,
                    'cid': 0,
                    'limit': list_limit,
                    'offset': 0,
                })
                items = self._extract_share_items(snap_resp)
            except Exception as e:
                return {
                    'success': False,
                    'message': f'读取分享列表失败: {e}',
                    'error': 'SHARE_SNAP_FAILED'
                }

            if not items:
                return {
                    'success': False,
                    'message': '分享链接内没有可转存的文件/目录',
                    'error': 'NO_ITEMS'
                }

            if len(items) == 1:
                selected_ids = [items[0]['id']]
            elif accept_all:
                selected_ids = [item['id'] for item in items]
            else:
                return {
                    'success': False,
                    'need_select': True,
                    'items': items,
                    'share_code': share_code,
                    'receive_code': receive_code,
                    'message': '分享链接包含多个项目，请选择要转存的条目',
                }

        try:
            recv_payload = {
                'share_code': share_code,
                'receive_code': receive_code,
                'file_id': ",".join(selected_ids),
            }
            if target_cid:
                recv_payload['cid'] = target_cid
            resp = self.p115_client.share_receive(recv_payload)
            if isinstance(resp, dict) and resp.get('state', True):
                return {
                    'success': True,
                    'message': '✅ 转存完成',
                    'response': resp,
                    'items': items,
                    'file_ids': selected_ids,
                }
            error_msg = ''
            if isinstance(resp, dict):
                error_msg = resp.get('error') or resp.get('message') or ''
            return {
                'success': False,
                'message': f'转存失败: {error_msg or resp}',
                'error': 'SHARE_RECEIVE_FAILED',
                'response': resp,
            }
        except Exception as e:
            return {
                'success': False,
                'message': f'转存失败: {e}',
                'error': 'SHARE_RECEIVE_EXCEPTION'
            }

    def check_file_exists_legacy(self, file_sha1: str, file_size: int, file_name: str) -> Dict[str, Any]:
        """
        检查文件是否秒传可用（旧版本，备用方案）
        
        Args:
            file_sha1: 文件SHA1哈希值
            file_size: 文件大小（字节）
            file_name: 文件名
        
        Returns:
            包含秒传结果的字典
        """
        # 使用新的方法实现
        return self.check_file_exists(file_sha1, file_size, file_name)
    
    def upload_to_oss(self, file_path: str, oss_credentials: Dict[str, Any]) -> Dict[str, Any]:
        """
        上传文件到阿里云OSS
        
        Args:
            file_path: 本地文件路径
            oss_credentials: OSS凭证（从check_file_exists返回）
        
        Returns:
            上传结果字典
        """
        import os
        import base64
        import json
        
        try:
            host = oss_credentials.get('host')
            object_key = oss_credentials.get('object')
            accessid = oss_credentials.get('accessid')
            policy = oss_credentials.get('policy')
            signature = oss_credentials.get('signature')
            callback_b64 = oss_credentials.get('callback', '')
            
            if not all([host, object_key, accessid, policy, signature]):
                return {
                    'success': False,
                    'message': 'OSS凭证不完整',
                    'error': 'INCOMPLETE_CREDENTIALS'
                }
            
            file_size = os.path.getsize(file_path)
            file_name = os.path.basename(file_path)
            
            print(f"\n[OSS上传] 开始上传文件到阿里云")
            print(f"文件: {file_name} ({file_size/1024/1024:.2f}MB)")
            print(f"目标: {host}/{object_key}")
            
            # 解析callback信息
            if callback_b64:
                try:
                    callback_decoded = base64.b64decode(callback_b64).decode('utf-8')
                    callback_info = json.loads(callback_decoded)
                    print(f"[回调] callbackUrl: {callback_info.get('callbackUrl')}")
                    print(f"[回调] 已配置OSS callback通知")
                except Exception as e:
                    print(f"[警告] callback解析失败: {e}，OSS可能不会自动通知115")
                    callback_b64 = ''
            
            # 准备multipart/form-data
            with open(file_path, 'rb') as f:
                files = {
                    'key': (None, object_key),
                    'policy': (None, policy),
                    'OSSAccessKeyId': (None, accessid),
                    'success_action_status': (None, '200'),
                    'signature': (None, signature),
                    'file': (file_name, f, 'application/octet-stream')
                }
                
                # 关键：必须包含callback，这样OSS才会在上传完成后通知115
                if callback_b64:
                    files['callback'] = (None, callback_b64)
                
                # 上传到OSS
                response = self.session.post(
                    host,
                    files=files,
                    timeout=300,  # 5分钟超时
                    verify=True
                )
            
            print(f"[OSS响应] HTTP {response.status_code}")
            if response.text:
                print(f"响应内容: {response.text[:500]}")
            else:
                print(f"响应内容: (空)")
            
            if response.status_code in [200, 201, 204]:
                print(f"✅ OSS上传成功（callback已配置，115服务器会自动接收完成通知）")
                return {
                    'success': True,
                    'message': 'OSS上传成功',
                    'object': object_key,
                    'response': response.text
                }
            else:
                print(f"❌ OSS上传失败: HTTP {response.status_code}")
                return {
                    'success': False,
                    'message': f'OSS上传失败 (HTTP {response.status_code})',
                    'error': f'HTTP_{response.status_code}',
                    'response': response.text[:500]
                }
                
        except Exception as e:
            print(f"[OSS上传异常] {str(e)}")
            import traceback
            traceback.print_exc()
            return {
                'success': False,
                'message': f'OSS上传异常: {str(e)}',
                'error': str(e)
            }
    
    def notify_upload_complete(self, file_sha1: str, file_size: int, file_name: str, target: str = 'U_1_0') -> Dict[str, Any]:
        """
        通知115服务器上传完成
        
        Args:
            file_sha1: 文件SHA1
            file_size: 文件大小
            file_name: 文件名
            target: 目标文件夹ID
        
        Returns:
            通知结果字典
        """
        if not self.cookie:
            return {
                'success': False,
                'message': '未设置Cookie',
                'error': 'NO_COOKIE'
            }
        
        print(f"\n[115完成通知] 通知服务器上传完成")
        print(f"文件: {file_name}")
        
        # 尝试115的完成接口
        try:
            # 方法1: 使用 uplbend.php 接口
            complete_url = 'https://uplb.115.com/3.0/uplbend.php'
            
            params = {
                'userid': self._extract_userid_from_cookie() or '',
                'filename': file_name,
                'filesize': file_size,
                'file_sha1': file_sha1,
                'target': target
            }
            
            enhanced_headers = {
                'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36',
                'Accept': 'application/json',
                'Referer': 'https://115.com/',
                'Origin': 'https://115.com',
                'Cookie': self.cookie
            }
            
            print(f"[请求] POST {complete_url}")
            print(f"参数: {params}")
            
            response = self.session.post(
                complete_url,
                data=params,
                headers=enhanced_headers,
                timeout=15,
                verify=True
            )
            
            print(f"[响应] HTTP {response.status_code}")
            
            try:
                response_data = response.json()
                print(f"[JSON响应] {json.dumps(response_data, ensure_ascii=False)[:500]}")
                
                if response.status_code == 200:
                    return {
                        'success': True,
                        'message': '通知完成成功',
                        'response': response_data
                    }
            except:
                pass
            
            return {
                'success': False,
                'message': '通知完成失败',
                'response': response.text[:500]
            }
            
        except Exception as e:
            print(f"[通知异常] {str(e)}")
            return {
                'success': False,
                'message': f'通知异常: {str(e)}',
                'error': str(e)
            }
    
    def upload_file(self, file_path: str, file_sha1: str, file_size: int, target: str = 'U_1_0') -> Dict[str, Any]:
        """
        完整的文件上传流程（检查秒传 → OSS上传）
        
        Args:
            file_path: 本地文件路径
            file_sha1: 文件SHA1
            file_size: 文件大小
            target: 目标文件夹ID
        
        Returns:
            上传结果字典
        """
        import os
        
        file_name = os.path.basename(file_path)
        
        print(f"\n{'='*60}")
        print(f"[115上传流程] {file_name}")
        print(f"{'='*60}")
        
        # 1. 检查是否可以秒传
        check_result = self.check_file_exists(file_sha1, file_size, file_name, target)
        
        # 如果秒传成功（文件已存在）
        if check_result.get('success') and check_result.get('can_transfer'):
            print(f"✅ 秒传成功，无需上传")
            return {
                'success': True,
                'message': f'秒传成功: {file_name}',
                'method': 'second_transfer',
                'response': check_result
            }
        
        # 如果需要上传到OSS
        if check_result.get('need_upload') and check_result.get('oss_credentials'):
            print(f"📤 文件不存在，开始上传到OSS")
            
            # 2. 上传到OSS（callback机制会自动通知115完成）
            oss_result = self.upload_to_oss(file_path, check_result['oss_credentials'])
            
            if oss_result.get('success'):
                print(f"✅ OSS上传完成（callback已配置，115服务器会自动接收完成通知）")
                return {
                    'success': True,
                    'message': f'上传成功: {file_name}',
                    'method': 'oss_upload',
                    'oss_response': oss_result,
                    'check_response': check_result
                }
            else:
                print(f"❌ OSS上传失败")
                return {
                    'success': False,
                    'message': f'上传失败: {file_name}',
                    'error': oss_result.get('error'),
                    'oss_response': oss_result
                }
        
        # 其他错误
        return {
            'success': False,
            'message': f'上传失败: {check_result.get("message", "未知错误")}',
            'error': check_result.get('error'),
            'response': check_result
        }
    
    def get_user_info(self) -> Dict[str, Any]:
        """
        获取用户信息（验证登录状态）
        支持多个115网盘API端点进行验证
        
        Returns:
            用户信息字典
        """
        if not self.cookie:
            return {
                'success': False,
                'message': '未设置Cookie'
            }
        
        # 尝试多个API端点和请求方式验证
        verification_methods = [
            # 方法1: 标准用户信息接口
            {
                'name': '用户信息接口',
                'url': 'https://api.115.com/user',
                'method': 'GET',
                'params': {}
            },
            # 方法2: webapi用户信息
            {
                'name': 'WebAPI用户接口',
                'url': 'https://webapi.115.com/user',
                'method': 'GET',
                'params': {}
            },
            # 方法3: 用户资料接口
            {
                'name': '用户资料接口',
                'url': 'https://webapi.115.com/user/profile',
                'method': 'GET',
                'params': {}
            },
            # 方法4: 获取用户配额
            {
                'name': '用户配额接口',
                'url': 'https://api.115.com/user/quota',
                'method': 'GET',
                'params': {}
            },
            # 方法5: 文件列表（验证权限）
            {
                'name': '文件列表接口',
                'url': 'https://webapi.115.com/files',
                'method': 'GET',
                'params': {'limit': 1}
            },
            # 方法6: 目录列表
            {
                'name': '目录接口',
                'url': 'https://api.115.com/directory',
                'method': 'GET',
                'params': {'aid': 1}
            },
            # 方法7: API版本检查
            {
                'name': 'API版本接口',
                'url': 'https://api.115.com/version',
                'method': 'GET',
                'params': {}
            }
        ]
        
        for method_config in verification_methods:
            try:
                url = method_config['url']
                http_method = method_config['method']
                params = method_config['params']
                
                if http_method == 'GET':
                    response = self.session.get(url, params=params, timeout=10, allow_redirects=True)
                else:
                    response = self.session.post(url, json=params, timeout=10, allow_redirects=True)
                
                # 检查HTTP状态码
                if response.status_code in [200, 201, 204]:
                    try:
                        data = response.json()
                    except json.JSONDecodeError:
                        # 如果不是JSON，检查状态码是否表示成功
                        if response.status_code == 200:
                            return {
                                'success': True,
                                'message': f'Cookie验证成功（通过{method_config["name"]}）',
                                'user_info': {'verification_method': method_config['name']},
                                'response': {'status': 'verified', 'http_status': response.status_code}
                            }
                        continue
                    
                    # 处理不同的响应格式
                    if isinstance(data, dict):
                        # 检查多种可能的成功标识
                        success_indicators = [
                            (data.get('code') == 0, '成功响应格式1'),
                            (data.get('state') == 1, '成功响应格式2'),
                            (data.get('success') is True, '成功响应格式3'),
                            (data.get('ok') is True, '成功响应格式4'),
                            ('user_id' in data, '包含user_id'),
                            ('uid' in data, '包含uid'),
                            ('user_name' in data, '包含user_name'),
                            ('username' in data, '包含username'),
                            ('email' in data, '包含email'),
                            (data.get('status') == 'ok', 'status为ok'),
                            (response.status_code == 200 and len(data) > 0, 'HTTP200且有数据'),
                        ]
                        
                        for is_success, reason in success_indicators:
                            if is_success:
                                return {
                                    'success': True,
                                    'message': f'✅ Cookie验证成功（通过{method_config["name"]}）',
                                    'user_info': data.get('data', data),
                                    'response': data
                                }
                    
                    # 如果响应看起来像正常的JSON体，就认为验证成功
                    if response.status_code == 200 and isinstance(data, dict) and data:
                        return {
                            'success': True,
                            'message': f'✅ Cookie验证成功（通过{method_config["name"]}）',
                            'user_info': data,
                            'response': data
                        }
                
            except requests.exceptions.Timeout:
                continue
            except requests.exceptions.ConnectionError:
                continue
            except Exception as e:
                # 记录错误但继续尝试下一个方法
                continue
        
        # 如果所有验证都失败，返回友好的错误消息
        return {
            'success': False,
            'message': '❌ Cookie验证失败\n\n可能原因：\n1. Cookie已过期，请重新获取\n2. Cookie格式不正确\n3. 网络连接问题\n\n💡 解决方案：\n- 在115.com重新登录\n- 按F12打开开发者工具\n- 在Network标签中找任意请求\n- 复制完整的Cookie值（包括所有分号和空格）\n- 重新粘贴到此处验证',
            'error': '所有验证端点均失败',
            'tried_methods': [m['name'] for m in verification_methods]
        }
    
    def create_folder(self, folder_name: str, parent_id: str = '0') -> Dict[str, Any]:
        """
        创建文件夹
        
        Args:
            folder_name: 文件夹名称
            parent_id: 父文件夹ID
        
        Returns:
            创建结果字典
        """
        if not self.cookie:
            return {
                'success': False,
                'message': '未设置Cookie'
            }
        
        try:
            url = f'{self.base_url}/directory'
            
            data = {
                'name': folder_name,
                'pid': parent_id
            }
            
            response = self.session.post(url, json=data, timeout=10)
            response.raise_for_status()
            
            result = response.json()
            
            return {
                'success': result.get('code') == 0,
                'message': '文件夹创建成功' if result.get('code') == 0 else '创建失败',
                'response': result
            }
            
        except Exception as e:
            return {
                'success': False,
                'message': f'创建文件夹失败: {str(e)}',
                'error': str(e)
            }
