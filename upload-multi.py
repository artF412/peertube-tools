import requests
import os
import json
from pathlib import Path
import time
from typing import List, Dict

class PeerTubeUploader:
    def __init__(self, host: str, username: str, password: str):
        self.host = host.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self.channel_id = None
        self.channels = {}
        
    def login(self) -> None:
        """Get client credentials and login to obtain token"""
        # Get client credentials
        client_url = f"{self.host}/api/v1/oauth-clients/local"
        client_response = requests.get(client_url)
        client_response.raise_for_status()
        client_data = client_response.json()
        
        # Login to get token
        token_url = f"{self.host}/api/v1/users/token"
        token_data = {
            "client_id": client_data['client_id'],
            "client_secret": client_data['client_secret'],
            "grant_type": "password",
            "username": self.username,
            "password": self.password
        }
        
        response = requests.post(token_url, data=token_data)
        response.raise_for_status()
        
        self.token = response.json()['access_token']
        
        # Fetch channel information
        headers = {'Authorization': f'Bearer {self.token}'}
        response = requests.get(f"{self.host}/api/v1/users/me", headers=headers)
        response.raise_for_status()
        
        user_data = response.json()
        if 'videoChannels' in user_data and len(user_data['videoChannels']) > 0:
            self.channel_id = user_data['videoChannels'][0]['id']
            self.channels = {channel['name']: channel['id'] for channel in user_data['videoChannels']}
            print("Available channels:", self.channels)
        else:
            raise Exception("No video channels found")

    def create_channel(self, name: str, display_name: str = None, description: str = None) -> Dict:
        """Create a new channel"""
        if not self.token:
            self.login()
            
        url = f"{self.host}/api/v1/video-channels"
        headers = {'Authorization': f'Bearer {self.token}'}
        
        data = {
            'name': name,
            'displayName': display_name or name,
        }
        if description:
            data['description'] = description
            
        try:
            response = requests.post(url, headers=headers, json=data)
            response.raise_for_status()
            
            channel_data = response.json()
            self.channels[name] = channel_data['id']
            print(f"Created new channel: {name}")
            return channel_data
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 409:
                print("Channel might already exist:", e)
            else:
                raise

    def upload_video(self, video_path: str, title: str = None, description: str = None, 
                    privacy: int = 1, channel_name: str = None, wait_transcoding: bool = True) -> Dict:
        """
        Upload a video to PeerTube
        
        Args:
            video_path: Path to the video file
            title: Video title (default: use filename)
            description: Video description
            privacy: Privacy level (1=Private, 2=Unlisted, 3=Public)
            channel_name: Target channel name
            wait_transcoding: Wait for transcoding before publishing
        """
        if not self.token:
            self.login()

        video_file = Path(video_path)
        if not title:
            title = video_file.stem

        # Select channel
        channel_id = self.channels.get(channel_name, self.channel_id) if channel_name else self.channel_id

        # Prepare upload data
        headers = {
            'Authorization': f'Bearer {self.token}'
        }

        data = {
            'channelId': channel_id,
            'name': title,
            'privacy': privacy,
            'waitTranscoding': wait_transcoding
        }

        if description:
            data['description'] = description

        files = {
            'videofile': (video_file.name, open(video_path, 'rb'), 'video/mp4')
        }

        # Upload video
        print(f"Uploading {video_file.name}...")
        try:
            response = requests.post(
                f"{self.host}/api/v1/videos/upload",
                headers=headers,
                data=data,
                files=files,
                timeout=600  # Set timeout to 10 minutes
            )
            
            if response.status_code == 415:
                raise Exception(f"Video format not supported. Please use MP4 format with H.264 codec.")
            elif response.status_code == 413:
                raise Exception(f"Video file too large. Please check your quota.")
            elif response.status_code == 422:
                raise Exception(f"Video file unreadable. Please check the file.")
            
            response.raise_for_status()
            print(f"Successfully uploaded {video_file.name}")
            return response.json()
            
        except requests.exceptions.RequestException as e:
            print(f"Upload failed: {str(e)}")
            if hasattr(e.response, 'text'):
                print(f"Server response: {e.response.text}")
            raise

    def bulk_upload(self, video_dir: str, privacy: int = 1, channel_name: str = None) -> List[Dict]:
        """Upload all videos from a directory"""
        video_extensions = {'.mp4', '.webm', '.mkv'}
        results = []
        
        for file_path in Path(video_dir).iterdir():
            if file_path.suffix.lower() in video_extensions:
                try:
                    result = self.upload_video(
                        str(file_path),
                        privacy=privacy,
                        channel_name=channel_name,
                        wait_transcoding=True
                    )
                    results.append(result)
                    time.sleep(2)
                except Exception as e:
                    print(f"Error uploading {file_path.name}: {str(e)}")
                    continue
                    
        return results

# Example usage
if __name__ == "__main__":
    # Configure connection settings
    PEERTUBE_HOST = "{https://peertube.{Company_name}.com/}"
    USERNAME = "{USER}"
    PASSWORD = "{PASSWORD}"
    
    uploader = PeerTubeUploader(PEERTUBE_HOST, USERNAME, PASSWORD)
    
    try:
        uploader.create_channel(
            name="test_script",
            display_name="Test Script Channel",
            description="Channel for testing upload script"
        )
    except Exception as e:
        print(f"Channel creation error: {str(e)}")
    
    uploader.bulk_upload(
        video_dir="{Path}",
        privacy=1,  # private
        channel_name="{Channel_name}"
    )